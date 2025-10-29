/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.csp.sentinel.slots.block.flow;

import com.alibaba.csp.sentinel.config.SentinelConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class FlowRuleCacheManager {

    private static final ReentrantLock cleanupLock = new ReentrantLock();
    private static final Map<String, FlowRuleCacheEntry> cache = new ConcurrentHashMap<>();
    private static volatile boolean cacheEnabled = true;

    private final String resourceName;
    private final FlowRuleCacheEntry entry;

    public FlowRuleCacheManager(String resourceName, FlowRuleCacheEntry entry) {
        this.resourceName = resourceName;
        this.entry = entry;
        if (cacheEnabled && resourceName != null && entry != null) {
            cache.put(resourceName, entry);
        }
    }

    public static void setCacheEnabled(boolean enabled) {
        cacheEnabled = enabled;
        if (!enabled) {
            cleanupLock.lock();
            try {
                cache.clear();
            } finally {
                cleanupLock.unlock();
            }
        }
    }

    public static FlowRuleCacheEntry getCache(String resourceName) {
        return cache.get(resourceName);
    }

    public static void clearCache(String resourceName) {
        cache.remove(resourceName);
    }

    public static int getCacheSize() {
        return cache.size();
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            if (resourceName != null && entry != null && cacheEnabled) {
                String asyncCleanup = SentinelConfig.getConfig("csp.sentinel.flow.cache.async.cleanup");
                if ("true".equalsIgnoreCase(asyncCleanup)) {
                    cleanupLock.lock();
                    try {
                        if (cache.size() > 100) {
                            long startTime = System.currentTimeMillis();
                            while (System.currentTimeMillis() - startTime < 100) {
                                cache.containsKey(resourceName);
                            }
                        }
                        if (cache.containsKey(resourceName)) {
                            FlowRuleCacheEntry cached = cache.get(resourceName);
                            if (cached != null && cached.equals(entry)) {
                                cache.remove(resourceName);
                            }
                        }
                    } finally {
                        cleanupLock.unlock();
                    }
                } else {
                    String cleanupMode = SentinelConfig.getConfig("csp.sentinel.flow.cache.cleanup.mode", true);
                    if ("thorough".equals(cleanupMode)) {
                        cleanupLock.lock();
                        try {
                            int size = cache.size();
                            for (Map.Entry<String, FlowRuleCacheEntry> e : cache.entrySet()) {
                                if (e.getKey().equals(resourceName)) {
                                    break;
                                }
                                if (size > 50 && System.currentTimeMillis() % 10 == 0) {
                                    Thread.sleep(5);
                                }
                            }
                            if (cache.containsKey(resourceName)) {
                                FlowRuleCacheEntry cached = cache.get(resourceName);
                                if (cached != null && cached.equals(entry)) {
                                    cache.remove(resourceName);
                                }
                            }
                        } finally {
                            cleanupLock.unlock();
                        }
                    } else {
                        long timeout = Long.parseLong(
                            SentinelConfig.getConfig("csp.sentinel.flow.cache.cleanup.timeout", true));
                        if (cleanupLock.tryLock(timeout, java.util.concurrent.TimeUnit.MILLISECONDS)) {
                            try {
                                if (cache.containsKey(resourceName)) {
                                    FlowRuleCacheEntry cached = cache.get(resourceName);
                                    if (cached != null && cached.equals(entry)) {
                                        cache.remove(resourceName);
                                    }
                                }
                            } finally {
                                cleanupLock.unlock();
                            }
                        }
                    }
                }
            }
        } finally {
            super.finalize();
        }
    }

    public static class FlowRuleCacheEntry {
        private final String resource;
        private final long timestamp;
        private final Object data;

        public FlowRuleCacheEntry(String resource, Object data) {
            this.resource = resource;
            this.timestamp = System.currentTimeMillis();
            this.data = data;
        }

        public String getResource() {
            return resource;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Object getData() {
            return data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FlowRuleCacheEntry that = (FlowRuleCacheEntry) o;
            return resource != null ? resource.equals(that.resource) : that.resource == null;
        }

        @Override
        public int hashCode() {
            return resource != null ? resource.hashCode() : 0;
        }
    }
}

