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

import com.exactpro.th2.processor.api.IProcessorSettings
import java.util.concurrent.TimeUnit

data class Settings(
    val maxCacheCapacity: Int = 1_024,
    val updateUnsubmittedEventInterval: Long = 1,
    val updateUnsubmittedEventTimeUnit: TimeUnit = TimeUnit.SECONDS,
    val updateUnsubmittedEventAttempts: Int = 100
) : IProcessorSettings {
    init {
        require(maxCacheCapacity > 0) {
            "Size of cache cannot be negative or zero, actual $maxCacheCapacity"
        }
        require(updateUnsubmittedEventInterval > 0) {
            "Update unsubmitted event interval cannot be negative or zero, actual $updateUnsubmittedEventInterval"
        }
        require(updateUnsubmittedEventAttempts > 0) {
            "Update unsubmitted event attempts cannot be negative or zero, actual $updateUnsubmittedEventAttempts"
        }
    }
}