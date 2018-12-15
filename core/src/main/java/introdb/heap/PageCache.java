package introdb.heap;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

final class PageCache {
    private final Map<Integer, SoftReference<RecordPage>> numbersToPages;

    PageCache(int maxNrPages) {
        this.numbersToPages = new HashMap<>(maxNrPages);
    }

    void put(int pageNumber, RecordPage page) {
        numbersToPages.put(pageNumber, new SoftReference<>(page));
    }

    RecordPage get(int pageNumber) {
        var pageSoftReference = numbersToPages.get(pageNumber);
        if (pageSoftReference != null) {
            pageSoftReference.get();
        }
        return null;
    }

    void remove(int pageNumber) {
        numbersToPages.remove(pageNumber);
    }
}
