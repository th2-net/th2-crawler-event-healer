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

package com.exactpro.th2.processor.healer.state

import com.exactpro.cradle.BookId
import com.exactpro.cradle.testevents.StoredTestEventId
import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant


data class StateEventId(
    @JsonProperty("book") val book: String,
    @JsonProperty("scope") val scope: String,
    @JsonProperty("timestamp") val timestamp: Instant,
    @JsonProperty("id") val id: String
) {
    init {
        check(book.isNotBlank()) { "Book can't be blank, $this" }
        check(scope.isNotBlank()) { "Scope can't be blank, $this" }
        check(timestamp != Instant.MIN) { "Timestamp can't be equal as ${Instant.MIN}, $this" }
        check(id.isNotBlank()) { "Id can't be blank, $this" }
    }

    fun toStateEventId() = StoredTestEventId(BookId(book), scope, timestamp, id)
}
