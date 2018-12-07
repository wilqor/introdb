package introdb.heap;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CacheTest {
    @Test
    void keeps_most_recently_accessed_elements() {
        Cache<String, String> cache = new Cache<>(3);
        cache.put("one", "1");
        cache.put("two", "2");
        cache.put("three", "3");
        cache.put("deleted", "del");

        cache.invalidate("deleted");

        cache.get("one");

        cache.put("four", "4");

        assertEquals("1", cache.get("one"));
        assertEquals("3", cache.get("three"));
        assertEquals("4", cache.get("four"));
    }
}