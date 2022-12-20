/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.processor.healer

import com.exactpro.cradle.BookId
import com.exactpro.cradle.CradleStorage
import com.exactpro.cradle.testevents.StoredTestEventId
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.healer.state.State
import com.exactpro.th2.processor.utility.OBJECT_MAPPER
import com.exactpro.th2.processor.utility.log
import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledExecutorService

typealias EventBuilder = com.exactpro.th2.common.event.Event

class Processor(
    private val cradleStore: CradleStorage,
    private val scheduler: ScheduledExecutorService,
    private val eventBatcher: EventBatcher,
    processorEventId: EventID,
    configuration: Settings,
    serializedState: ByteArray?
) : IProcessor {

    private val interval = configuration.updateUnsubmittedEventInterval
    private val timeUtil = configuration.updateUnsubmittedEventTimeUnit
    private val attempts = configuration.updateUnsubmittedEventAttempts

    private val unsubmittedEvents: MutableMap<StoredTestEventId, Int> = ConcurrentHashMap()

    private val statusCache: Cache<StoredTestEventId, Any> = Caffeine.newBuilder()
        .maximumSize(configuration.maxCacheCapacity.toLong())
        .executor(scheduler)
        .build()

    init {
        serializedState?.let {
            OBJECT_MAPPER.readValue(it, State::class.java)
                .unsubmittedEvents.forEach { stateEventId ->
                    val sickEventId = stateEventId.toStateEventId()
                    heal(processorEventId, null, sickEventId)
                }
        }
    }

    override fun handle(intervalEventId: EventID, event: Event) {
        if (event.status == EventStatus.SUCCESS || !event.hasParentId()) {
            return
        }

        heal(
            intervalEventId,
            event.id.toStoredTestEventId(),
            event.parentId.toStoredTestEventId()
        )
    }

    override fun serializeState(): ByteArray? = if (unsubmittedEvents.isEmpty()) {
        null
    } else {
        OBJECT_MAPPER.writeValueAsBytes(unsubmittedEvents.keys.toState())
    }

    override fun close() { }

    /**
     * @return true if at least the first is healed
     */
    private fun heal(
        reportEventId: EventID,
        childEventId: StoredTestEventId?,
        parentEventId: StoredTestEventId?
    ): Boolean {
        var sickEventId = parentEventId
        var result = false
        while (sickEventId != null) {
            if (statusCache.getIfPresent(sickEventId) === FAKE_OBJECT) {
                K_LOGGER.debug { "The $sickEventId has been already updated${ childEventId?.let { " for $childEventId child event id" } ?: "" }" }
                sickEventId = null
            } else {
                val parentEvent = cradleStore.getTestEvent(sickEventId)
                if (parentEvent == null) {
                    val attempt = unsubmittedEvents.compute(sickEventId) { _, previous ->
                        when (val next = (previous ?: 0) + 1) {
                            attempts -> null
                            else     -> next
                        }
                    }
                    when (attempt) {
                        null    -> reportUnhealedEvent(reportEventId, sickEventId)
                        else    -> scheduleHeal(reportEventId, childEventId, sickEventId, attempt)
                    }
                    sickEventId = null
                } else {
                    result = true
                    sickEventId = if (parentEvent.isSuccess) {
                        cradleStore.updateEventStatus(parentEvent, false) //FIXME: push sub-event for updated, catch exception
                        reportUpdateEvent(reportEventId, parentEvent.id)
                        unsubmittedEvents.remove(sickEventId)
                        parentEvent.parentId
                    } else {
                        K_LOGGER.debug {
                            "The ${parentEvent.id} has already has failed status${ childEventId?.let { " for $childEventId child event id" } ?: "" }"
                        }
                        null
                    }
                    statusCache.put(parentEvent.id, FAKE_OBJECT)
                }
            }
        }
        return result
    }

    private fun scheduleHeal(
        reportEventId: EventID,
        childEventId: StoredTestEventId?,
        sickEventId: StoredTestEventId,
        attempt: Int
    ) {
        scheduler.schedule({
                runCatching {
                    heal(reportEventId, childEventId, sickEventId)
                }.onFailure { e ->
                    K_LOGGER.error(e) { "Failure to heal $sickEventId event" }
                    reportErrorEvent(reportEventId, sickEventId, e)
                }
            }, interval, timeUtil
        )
        reportUnsubmittedEvent(reportEventId, sickEventId, attempt)
    }

    private fun reportUnhealedEvent(reportEventId: EventID, eventId: StoredTestEventId) {
        eventBatcher.onEvent(
            EventBuilder.start()
                .name(
                    "Heal of $eventId event failure after $attempts attempts with interval $interval $timeUtil"
                )
                .type(EVENT_TYPE_UNSUBMITTED_EVENT)
                .status(FAILED)
                .toProto(reportEventId)
                .log(K_LOGGER)
        )
    }

    private fun reportErrorEvent(reportEventId: EventID, eventId: StoredTestEventId, e: Throwable) {
        // FIXME: Add link to updated event
        eventBatcher.onEvent(
            EventBuilder.start()
                .name("Heal of $eventId event failure")
                .type(EVENT_TYPE_INTERNAL_ERROR)
                .exception(e, true)
                .status(FAILED)
                .toProto(reportEventId)
                .log(K_LOGGER)
        )
    }

    private fun reportUnsubmittedEvent(intervalEventId: EventID, eventId: StoredTestEventId, attempt: Int) {
        // FIXME: Add link to updated event
        eventBatcher.onEvent(
            EventBuilder.start()
                .name("The $eventId hasn't been submitted to cradle yet, attempt $attempt")
                .type(EVENT_TYPE_UNSUBMITTED_EVENT)
                .toProto(intervalEventId)
                .log(K_LOGGER)
        )
    }

    private fun reportUpdateEvent(intervalEventId: EventID, eventId: StoredTestEventId) {
        // FIXME: Add link to updated event
        eventBatcher.onEvent(
            EventBuilder.start()
                .name("Updated status of $eventId")
                .type("Update status")
                .toProto(intervalEventId)
        )
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
        private val FAKE_OBJECT: Any = Object()
        private const val EVENT_TYPE_INTERNAL_ERROR: String = "Internal error"
        private const val EVENT_TYPE_UNSUBMITTED_EVENT: String = "Unsubmitted event"

        private fun EventID.toStoredTestEventId(): StoredTestEventId = StoredTestEventId(
            BookId(bookName),
            scope,
            startTimestamp.toInstant(),
            id
        )
    }
}