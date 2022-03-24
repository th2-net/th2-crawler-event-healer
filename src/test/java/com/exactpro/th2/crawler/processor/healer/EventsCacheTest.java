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

import com.exactpro.th2.crawler.processor.healer.cache.EventsCache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EventsCacheTest {
    private final Map<String, Integer> cache = new EventsCache<>(10);

    @BeforeEach
    public void prepare() {
        for (int i = 0; i < 11; i++) {
            cache.put(String.valueOf(i), i);
        }
    }

    @Test
    public void maxSizeTest() { assertEquals(cache.size(), 10); }

    @Test
    public void firstElementRemoved() { assertFalse(cache.containsValue(0)); }

    @Test
    public void accessedElementPutLast() {
        cache.get("1");

        cache.put(String.valueOf(12), 12);

        assertTrue(cache.containsKey("1"), () -> "Cache must contain 1 but doesn't: " + cache);
    }
}
