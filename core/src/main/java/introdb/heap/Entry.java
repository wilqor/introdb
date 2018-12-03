package introdb.heap;

import java.io.Serializable;
import java.util.Objects;

final class Entry {
    private final Serializable key;
    private final Serializable value;

    Entry(Serializable key, Serializable value) {
        this.key = key;
        this.value = value;
    }

    Serializable key() {
        return key;
    }

    Serializable value() {
        return value;
    }

    @Override
    public String toString() {
        return "Entry [key=" + key + ", value=" + value + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Entry entry = (Entry) o;
        return Objects.equals(key, entry.key) &&
                Objects.equals(value, entry.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }
}
