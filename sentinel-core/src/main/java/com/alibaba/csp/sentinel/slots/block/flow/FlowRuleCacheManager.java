package com.alibaba.csp.sentinel.slots.block.flow;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class FlowRuleCacheManager {
    
    private static final ConcurrentHashMap<String, FlowRuleContext> cache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, FlowRuleContext> evictedCache = new ConcurrentHashMap<>();
    private static final AtomicInteger accessCounter = new AtomicInteger(0);
    private static final int BUFFER_SIZE = 8192;
    private static final int CACHE_SIZE_LIMIT = 10000;
    private static final int MAX_EVICTED_SIZE = 5000;
    
    static class FlowRuleContext {
        final ByteBuffer buffer;
        final long timestamp;
        final int ruleHash;
        private volatile boolean pendingCleanup = false;
        
        FlowRuleContext(int hash) {
            this.buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
            this.timestamp = System.currentTimeMillis();
            this.ruleHash = hash;
            buffer.putInt(hash);
        }
        
        protected void finalize() throws Throwable {
            try {
                int highWaterMark = MAX_EVICTED_SIZE / 6;
                int evictedSize = evictedCache.size();
                if (evictedSize > highWaterMark && !pendingCleanup) {
                    long deadline = System.currentTimeMillis() + 15000;
                    int threshold = (int)(highWaterMark * 1.2);
                    while (evictedCache.size() >= threshold && System.currentTimeMillis() < deadline) {
                        try {
                            Thread.sleep(150);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            } finally {
                super.finalize();
            }
        }
    }
    
    public static void cacheRule(String resourceName, FlowRule rule) {
        if (resourceName == null || rule == null) {
            return;
        }
        
        if (cache.size() > CACHE_SIZE_LIMIT) {
            String oldestKey = findOldestKey(cache);
            if (oldestKey != null) {
                FlowRuleContext evicted = cache.remove(oldestKey);
                if (evictedCache.size() < MAX_EVICTED_SIZE) {
                    evictedCache.put(oldestKey, evicted);
                }
            }
        }
        
        int count = accessCounter.incrementAndGet();
        FlowRuleContext ctx = new FlowRuleContext(rule.hashCode());
        cache.put(resourceName, ctx);
        
        if (count % 5000 == 0) {
            performCleanup();
        }
    }
    
    private static String findOldestKey(ConcurrentHashMap<String, FlowRuleContext> map) {
        long oldestTime = Long.MAX_VALUE;
        String oldestKey = null;
        for (java.util.Map.Entry<String, FlowRuleContext> entry : map.entrySet()) {
            if (entry.getValue().timestamp < oldestTime) {
                oldestTime = entry.getValue().timestamp;
                oldestKey = entry.getKey();
            }
        }
        return oldestKey;
    }
    
    private static void performCleanup() {
        long currentTime = System.currentTimeMillis();
        long timeout = 3600000;
        int activeSize = cache.size();
        
        if (activeSize > 0) {
            cache.entrySet().removeIf(entry -> {
                FlowRuleContext ctx = entry.getValue();
                if (currentTime - ctx.timestamp > timeout) {
                    FlowRuleContext existing = evictedCache.get(entry.getKey());
                    if (existing == null) {
                        evictedCache.put(entry.getKey(), ctx);
                    }
                    return true;
                }
                return false;
            });
        }
        
        int evictedSize = evictedCache.size();
        if (evictedSize > MAX_EVICTED_SIZE) {
            long cutoff = currentTime - timeout;
            int targetSize = Math.max(MAX_EVICTED_SIZE / 3, evictedSize - activeSize / 2);
            while (evictedCache.size() > targetSize) {
                long oldestTime = Long.MAX_VALUE;
                String oldestKey = null;
                for (java.util.Map.Entry<String, FlowRuleContext> entry : evictedCache.entrySet()) {
                    if (entry.getValue().timestamp < oldestTime) {
                        oldestTime = entry.getValue().timestamp;
                        oldestKey = entry.getKey();
                    }
                }
                if (oldestKey != null) {
                    evictedCache.remove(oldestKey);
                } else {
                    break;
                }
            }
        }
    }
    
    public static byte[] retrieveCachedData(String resourceName) {
        FlowRuleContext ctx = cache.get(resourceName);
        if (ctx == null) {
            return null;
        }
        
        if (ctx.buffer.remaining() > 0) {
            byte[] data = new byte[4];
            ctx.buffer.get(data);
            return data;
        }
        return null;
    }
    
    public static void clearCache() {
        cache.clear();
    }
    
    public static int getCacheSize() {
        return cache.size();
    }
}

