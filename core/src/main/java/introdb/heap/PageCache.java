package introdb.heap;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

final class PageCache {
    private final Map<Integer, SoftReference<RecordPage>> numbersToPages;
    private final int pageSize;

    PageCache(int maxNrPages, int pageSize) {
        this.numbersToPages = new HashMap<>(maxNrPages);
        this.pageSize = pageSize;
    }

    void put(int pageNumber, RecordPage page) {
        numbersToPages.put(pageNumber, new SoftReference<>(new RecordPage(page, ByteBuffer.allocate(pageSize))));
    }

    RecordPage get(int pageNumber, ByteBuffer buffer) {
        var pageSoftReference = numbersToPages.get(pageNumber);
        if (pageSoftReference != null) {
            RecordPage recordPage = pageSoftReference.get();
            if (recordPage != null) {
                return new RecordPage(recordPage, buffer);
            }
        }
        return null;
    }

    void remove(int pageNumber) {
        numbersToPages.remove(pageNumber);
    }
}
