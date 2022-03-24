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

package com.exactpro.th2.crawler.processor.healer;

import com.exactpro.cradle.CradleStorage;
import com.exactpro.th2.crawler.processor.healer.cfg.HealerConfiguration;
import com.exactpro.th2.crawler.processor.healer.grpc.HealerImpl;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter;
import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.exactpro.th2.common.metrics.CommonMetrics.setLiveness;
import static com.exactpro.th2.common.metrics.CommonMetrics.setReadiness;

public class BoxMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(BoxMain.class);

    public static void main(String[] args) {
        Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        configureShutdownHook(resources, lock, condition);
        try {
            setLiveness(true);

            CommonFactory factory = CommonFactory.createFromArguments(args);
            resources.add(factory);

            GrpcRouter grpcRouter = factory.getGrpcRouter();
            resources.add(grpcRouter);

            HealerConfiguration configuration = factory.getCustomConfiguration(HealerConfiguration.class);
            CradleStorage storage = factory.getCradleManager().getStorage();

            HealerImpl handler = new HealerImpl(configuration, storage);

            Server server = grpcRouter.startServer(handler).start();
            resources.add(() -> {
                LOGGER.info("Shutting down Healer gRPC server");

                TimeUnit unit = TimeUnit.SECONDS;
                long timeout = 5L;

                if (server.shutdown().awaitTermination(timeout, unit)) {
                    LOGGER.warn("Cannot shutdown server in {} millis. Shutdown now", unit.toMillis(timeout));
                    server.shutdownNow();
                }
            });

            setReadiness(true);

            LOGGER.info("Healer started");

            awaitShutdown(lock, condition);
        } catch (InterruptedException e) {
            LOGGER.info("The main thread interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.error("Fatal error: " + e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void awaitShutdown(ReentrantLock lock, Condition condition) throws InterruptedException {
        try {
            lock.lock();
            LOGGER.info("Wait shutdown");
            condition.await();
            LOGGER.info("App shutdown");
        } finally {
            lock.unlock();
        }
    }

    private static void configureShutdownHook(Deque<AutoCloseable> resources, ReentrantLock lock, Condition condition) {
        Runtime.getRuntime().addShutdownHook(new Thread("Shutdown hook") {
            @Override
            public void run() {
                LOGGER.info("Shutdown start");
                setReadiness(false);
                try {
                    lock.lock();
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }

                resources.descendingIterator().forEachRemaining(resource -> {
                    try {
                        resource.close();
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
                setLiveness(false);
                LOGGER.info("Shutdown end");
            }
        });
    }

}
