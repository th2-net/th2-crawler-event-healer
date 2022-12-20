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

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.api.IProcessorSettings
import com.exactpro.th2.processor.api.ProcessorContext
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.google.auto.service.AutoService
import java.time.Instant

@Suppress("unused")
@AutoService(IProcessorFactory::class)
class Factory : IProcessorFactory {
    override fun registerModules(configureMapper: ObjectMapper) {
        with(configureMapper) {
            registerModule(SimpleModule().addAbstractTypeMapping(IProcessorSettings::class.java, Settings::class.java))
        }
    }

    override fun create(context: ProcessorContext): IProcessor {
        with(context) {
            requireNotNull(settings) {
                "Settings can not be null"
            }.let { settings ->
                check(settings is Settings) {
                    "Settings type mismatch expected: ${Settings::class}, actual: ${settings::class}"
                }
                return Processor(
                    commonFactory.cradleManager.storage,
                    scheduler,
                    eventBatcher,
                    processorEventId,
                    settings,
                    state
                )
            }
        }
    }

    override fun createProcessorEvent(): Event = Event.start()
        .name("Healer event data processor ${Instant.now()}")
        .description("Will contain all events with errors and information about processed events")
        .type("Microservice")
}