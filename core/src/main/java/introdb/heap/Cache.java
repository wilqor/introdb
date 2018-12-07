package introdb.heap;

import java.util.LinkedHashMap;
import java.util.Map;

final class Cache<K, V> {
    private final LRUHashMap<K, V> map;

    Cache(int maxEntries) {
        map = new LRUHashMap<>(maxEntries);
    }

    void put(K key, V value) {
        map.put(key, value);
    }

    void invalidate(K key) {
        map.remove(key);
    }

    V get(K key) {
        return map.get(key);
    }

    private static final class LRUHashMap<K, V> extends LinkedHashMap<K, V> {
        private static final int DEFAULT_INITIAL_CAPACITY = 16;
        private static final float DEFAULT_LOAD_FACTOR = 0.75f;

        private final int maxEntries;

        private LRUHashMap(int maxEntries) {
            // accessOrder ordering mode
            super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, true);
            this.maxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > maxEntries;
        }
    }
}
