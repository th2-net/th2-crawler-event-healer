/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.crawler.processor.healer.grpc;

import static com.exactpro.th2.common.message.MessageUtils.toJson;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.testevents.StoredTestEventId;
import com.exactpro.cradle.testevents.StoredTestEventWrapper;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.EventStatus;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerId;
import com.exactpro.th2.crawler.dataprocessor.grpc.CrawlerInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorGrpc;
import com.exactpro.th2.crawler.dataprocessor.grpc.DataProcessorInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventDataRequest;
import com.exactpro.th2.crawler.dataprocessor.grpc.EventResponse;
import com.exactpro.th2.crawler.dataprocessor.grpc.IntervalInfo;
import com.exactpro.th2.crawler.dataprocessor.grpc.Status;
import com.exactpro.th2.crawler.processor.healer.cfg.HealerConfiguration;
import com.exactpro.th2.dataprovider.grpc.EventData;
import com.exactpro.th2.crawler.processor.healer.cache.EventsCache;
import com.google.protobuf.Empty;
import io.prometheus.client.Counter;

import io.grpc.stub.StreamObserver;

public class HealerImpl extends DataProcessorGrpc.DataProcessorImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealerImpl.class);

    private final HealerConfiguration configuration;
    private final CradleStorage storage;
    private final Map<String, InnerEvent> cache;
    private final Set<CrawlerId> knownCrawlers = ConcurrentHashMap.newKeySet();
    private final long waitParent;
    private final long waitingStep;
    private Set<String> notFoundParent;

    private static final Counter CRAWLER_PROCESSOR_EVENT_CORRECTED_STATUS_TOTAL = Counter.build()
            .name("th2_crawler_processor_event_corrected_status_total")
            .help("Quantity of events with corrected statuses")
            .register();

    private static final Counter CRAWLER_PROCESSOR_EVENT_NOT_FOUND_TOTAL = Counter.build()
            .name("th2_crawler_processor_event_not_found_total")
            .help("Quantity of events with not found id of a parent event")
            .register();

    public HealerImpl(HealerConfiguration configuration, CradleStorage storage) {
        this.configuration = requireNonNull(configuration, "Configuration cannot be null");
        this.storage = requireNonNull(storage, "Cradle storage cannot be null");
        this.cache = new EventsCache<>(configuration.getMaxCacheCapacity());
        this.waitParent = Duration.of(configuration.getWaitParent(), configuration.getWaitParentOffsetUnit()).toMillis();
        this.waitingStep = Duration.of(configuration.getWaitingStep(), configuration.getWaitingStepOffsetUnit()).toMillis();
    }

    @Override
    public void crawlerConnect(CrawlerInfo request, StreamObserver<DataProcessorInfo> responseObserver) {
        try {

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("crawlerConnect request: {}", toJson(request, true));
            }

            knownCrawlers.add(request.getId());

            DataProcessorInfo response = DataProcessorInfo.newBuilder()
                    .setName(configuration.getName())
                    .setVersion(configuration.getVersion())
                    .build();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("crawlerConnect response: {}", toJson(response, true));
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
            LOGGER.error("crawlerConnect error: " + e.getMessage(), e);
        }
    }

    @Override
    public void intervalStart(IntervalInfo request, StreamObserver<Empty> responseObserver) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("intervalStart request: {}", toJson(request));
        }
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void sendEvent(EventDataRequest request, StreamObserver<EventResponse> responseObserver) {
        try {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("sendEvent request: {}", toJson(request, true));
            }

            if (!knownCrawlers.contains(request.getId())) {

                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Received request from unknown crawler with id {}. Sending response with HandshakeRequired = true", toJson(request.getId(), true));
                }

                responseObserver.onNext(EventResponse.newBuilder()
                        .setStatus(Status.newBuilder().setHandshakeRequired(true))
                        .build());
                responseObserver.onCompleted();
                return;
            }

            int eventsCount = request.getEventDataCount();

            heal(request.getEventDataList());

            EventID lastEventId = null;

            if (eventsCount > 0) {
                lastEventId = request.getEventDataList().get(eventsCount - 1).getEventId();
            }

            EventResponse.Builder builder = EventResponse.newBuilder();

            if (lastEventId != null) {
                builder.setId(lastEventId);
            }

            EventResponse response = builder.build();

            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("sendEvent response: {}", toJson(response, true));
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
            LOGGER.error("sendEvent error: " + e, e);
        }
    }

    private void heal(Collection<EventData> events) throws IOException, InterruptedException {
        notFoundParent = new HashSet<>();
        int quantityHealed = 0;

        List<InnerEvent> eventAncestors;
        for (EventData event : events) {
            if (event.getSuccessful() == EventStatus.FAILED && event.hasParentEventId()) {
                eventAncestors = getAncestors(event);

                for (InnerEvent ancestor : eventAncestors) {
                    StoredTestEventWrapper ancestorEvent = ancestor.event;

                    if (ancestor.success) {
                        quantityHealed++;
                        storage.updateEventStatus(ancestorEvent, false);
                        ancestor.markFailed();
                        LOGGER.info("Event {} healed", ancestorEvent.getId());
                    }
                }
            }
        }

        if (!notFoundParent.isEmpty()){
            CRAWLER_PROCESSOR_EVENT_NOT_FOUND_TOTAL.inc(notFoundParent.size());
        }
        if (quantityHealed > 0){
            CRAWLER_PROCESSOR_EVENT_CORRECTED_STATUS_TOTAL.inc(quantityHealed);
        }
    }

    private List<InnerEvent> getAncestors(EventData event) throws IOException, InterruptedException {
        List<InnerEvent> eventAncestors = new ArrayList<>();
        String parentId = event.getParentEventId().getId();

        while (parentId != null) {
            InnerEvent innerEvent;

            if (cache.containsKey(parentId)) {
                innerEvent = cache.get(parentId);
            } else {
                StoredTestEventWrapper parent = storage.getTestEvent(new StoredTestEventId(parentId));
                if (parent == null) {
                    if (notFoundParent.contains(parentId)) {
                        LOGGER.debug("Parent element {} none", parentId);
                        return eventAncestors;
                    }

                    long from = System.currentTimeMillis();
                    long to = from + waitParent;

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Waiting for parentEventId in an interval of size {} with a step {}.", Duration.ofMillis(waitParent), Duration.ofMillis(waitingStep));
                    }

                    while(from < to) {
                        Thread.sleep(Math.min(waitingStep, to - from));

                        parent = storage.getTestEvent(new StoredTestEventId(parentId));
                        if (parent == null) {
                            from += waitingStep;
                        } else {
                            break;
                        }
                    }

                    if (parent == null) {
                        LOGGER.debug("Failed to extract test event data {}", parentId);
                        notFoundParent.add(parentId);
                        return eventAncestors;
                    }
                } else {
                    notFoundParent.remove(parentId);
                }

                innerEvent = new InnerEvent(parent, parent.isSuccess());
                cache.put(parentId, innerEvent);
            }

            eventAncestors.add(innerEvent);

            if (!innerEvent.success) break;

            StoredTestEventId eventId = innerEvent.event.getParentId();

            if (eventId == null) break;
            parentId = eventId.toString();
        }

        return eventAncestors;
    }

    private static class InnerEvent {
        private final StoredTestEventWrapper event;
        private volatile boolean success;

        private InnerEvent(StoredTestEventWrapper event, boolean success) {
            this.event = event;
            this.success = success;
        }

        private void markFailed() { this.success = false; }
    }
}
