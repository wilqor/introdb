package introdb.heap;

class PageWithRecord {
    private final EntryPage page;
    private final PageRecord record;

    PageWithRecord(EntryPage page, PageRecord record) {
        this.page = page;
        this.record = record;
    }

    EntryPage getPage() {
        return page;
    }

    PageRecord getRecord() {
        return record;
    }

    @Override
    public String toString() {
        return "PageWithRecord{" +
                "page=" + page +
                ", record=" + record +
                '}';
    }
}
