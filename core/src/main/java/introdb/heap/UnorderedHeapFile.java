package introdb.heap;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class UnorderedHeapFile implements Store {

    private final Path path;
    private final int pageSize;
    private final int maxNrPages;

    UnorderedHeapFile(Path path, int maxNrPages, int pageSize) {
        this.path = path;
        this.maxNrPages = maxNrPages;
        this.pageSize = pageSize;
    }

    @Override
    public void put(Entry entry) throws IOException, ClassNotFoundException {
        remove(entry.key());
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            long currentSize = fileChannel.size();
            long fullPagesCount = currentSize / pageSize;
            long remainingPageSize = currentSize % pageSize;
            long totalPagesCount = fullPagesCount + (remainingPageSize > 0 ? 1 : 0);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(entry.key());
            oos.writeObject(entry.value());
            byte[] entryBytes = out.toByteArray();
            int entrySize = entryBytes.length;
            // 1 byte flag + 2 bytes size = 3
            int totalObjectSize = entrySize + 3;
            if (totalObjectSize > pageSize) {
                throw new IllegalArgumentException("Too large object!");
            }
            if ((pageSize - remainingPageSize) >= totalObjectSize) {
                // write on last page
                fileChannel.position(currentSize);
            } else {
                // write to new page
                if (totalPagesCount == maxNrPages) {
                    throw new IllegalArgumentException("Max number of pages reached!");
                }
                fileChannel.position(totalPagesCount * pageSize);
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            // not deleted
            byteBuffer.put((byte) 0);
            // size
            byteBuffer.putShort((short) entrySize);
            byteBuffer.put(entryBytes);
            byteBuffer.flip();
            fileChannel.write(byteBuffer);
        }
    }

    @Override
    public Object get(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            while (fileChannel.read(byteBuffer) > 0) {
                // flip for reading
                byteBuffer.flip();
                while (byteBuffer.hasRemaining()) {
                    byte deletedFlag = byteBuffer.get();
                    short entrySize = byteBuffer.getShort();
                    // page incomplete
                    if (entrySize == 0) {
                        break;
                    }
                    if (deletedFlag == 1) {
                        // skip
                        byteBuffer.position(byteBuffer.position() + entrySize);
                    } else {
                        byte[] entryBytes = new byte[entrySize];
                        byteBuffer.get(entryBytes);
                        ByteArrayInputStream in = new ByteArrayInputStream(entryBytes);
                        ObjectInputStream ois = new ObjectInputStream(in);
                        // key
                        Serializable currentKey = (Serializable) ois.readObject();
                        if (key.equals(currentKey)) {
                            // value
                            return ois.readObject();
                        }
                    }
                }
                byteBuffer.clear();
            }
            return null;
        }
    }

    public Object remove(Serializable key) throws IOException, ClassNotFoundException {
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            ByteBuffer byteBuffer = ByteBuffer.allocate(pageSize);
            while (fileChannel.read(byteBuffer) > 0) {
                // flip for reading
                byteBuffer.flip();
                while (byteBuffer.hasRemaining()) {
                    byte deletedFlag = byteBuffer.get();
                    short entrySize = byteBuffer.getShort();
                    // page incomplete
                    if (entrySize == 0) {
                        break;
                    }
                    if (deletedFlag == 1) {
                        // skip
                        byteBuffer.position(byteBuffer.position() + entrySize);
                    } else {
                        byte[] entryBytes = new byte[entrySize];
                        byteBuffer.get(entryBytes);
                        ByteArrayInputStream in = new ByteArrayInputStream(entryBytes);
                        ObjectInputStream ois = new ObjectInputStream(in);
                        Serializable currentKey = (Serializable) ois.readObject();
                        if (key.equals(currentKey)) {
                            // entry value + flag + size
                            int objectLength = entrySize + 3;
                            byteBuffer.put(byteBuffer.position() - objectLength, (byte) 1);
                            byteBuffer.flip();
                            // move back the limit
                            fileChannel.position(fileChannel.position() - objectLength);
                            fileChannel.write(byteBuffer);
                            return ois.readObject();
                        }
                    }
                }
                byteBuffer.clear();
            }
            return null;
        }
    }

}

