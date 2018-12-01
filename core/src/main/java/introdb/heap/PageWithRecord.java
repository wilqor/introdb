package introdb.heap;

class PageWithRecord {
    private final EntryPage page;
    private final EntryRecord record;

    PageWithRecord(EntryPage page, EntryRecord record) {
        this.page = page;
        this.record = record;
    }

    EntryPage getPage() {
        return page;
    }

    EntryRecord getRecord() {
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
