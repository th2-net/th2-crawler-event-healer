/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.api.IProcessorSettings
import com.google.auto.service.AutoService

@Suppress("unused")
@AutoService(IProcessorFactory::class)
class Factory : IProcessorFactory {
    override val settingsClass: Class<out IProcessorSettings> = Settings::class.java

    override fun create(
        commonFactory: AbstractCommonFactory,
        eventBatcher: EventBatcher,
        processorEventId: EventID,
        settings: IProcessorSettings?,
        state: ByteArray?
    ): IProcessor {
        settings?.let {
            check(settings is Settings) {
                "Settings type mismatch expected: ${Settings::class}, actual: ${settings::class}"
            }
            return Processor(commonFactory.cradleManager.storage, eventBatcher, settings)
        }?: error("Settings can not be null")
    }
}