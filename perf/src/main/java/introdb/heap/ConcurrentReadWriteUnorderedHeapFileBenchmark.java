package introdb.heap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class ConcurrentReadWriteUnorderedHeapFileBenchmark {
		
	private static final int INITIAL_KEYS = 100_000;

	@State(Scope.Thread)
	public static class NextReadKey {
		int nextReadKey = 0;
		
		@TearDown(Level.Iteration)
		public void tearDown() {
			nextReadKey = 0;
		}
	}
	
	@State(Scope.Thread)
	public static class NextWriteKey {
		int nextWriteKey = INITIAL_KEYS;
		
		@TearDown(Level.Iteration)
		public void tearDown() {
			nextWriteKey = INITIAL_KEYS;
		}
	}

	private static final int MAX_PAGES = 100_000;
	private static final int MAX_RANDOM_KEYS = 10_000_000;
	
	@Param( {"512","1024","2048"})
	public int bufferSize; 
	private Store heapFile;
	private Path tempFile;
	
	
	@Setup(Level.Iteration)
	public void setUp() throws IOException, ClassNotFoundException {
						
		tempFile = Files.createTempFile("heap", "0001");
		heapFile = new UnorderedHeapFile(tempFile, MAX_PAGES, 4*1024);

		int i;
		for(i = 0;i<INITIAL_KEYS;i++) {
			heapFile.put(new Entry(toArrayWithPadding(i, 64), toArrayWithPadding(i, bufferSize)));
		}
	}
	
	@TearDown(Level.Iteration)
	public void tearDown() throws IOException{
		Files.delete(tempFile);
	}
	
    @Benchmark
    @Group("concurrent_read_write")
    public int writeEntry(NextWriteKey nextWriteKey) throws ClassNotFoundException, IOException {
    	int newKey = nextWriteKey.nextWriteKey++;
    	heapFile.put(new Entry(toArrayWithPadding(newKey, 64), toArrayWithPadding(newKey, bufferSize)));
    	return newKey;
    }

    @Benchmark
    @Group("concurrent_read_write")
    public void readEntry(NextReadKey nextReadKey, Blackhole blackhole) {
		try {
			Integer key = nextReadKey.nextReadKey++;
			byte[] bytes = (byte[]) heapFile.get(toArrayWithPadding(key, 64));
			blackhole.consume(bytes);
			if(bytes!=null && !Arrays.equals(bytes, toArrayWithPadding(key, bufferSize))) {
				throw new IllegalStateException("heap file is corrupted");
			}    		
		} catch (ClassNotFoundException | IOException e) {
			throw new RuntimeException(e);
		}
    }
    
    byte[] toArrayWithPadding(int value, int padding) {
    	byte[] bytes = Integer.toString(value).getBytes();
    	return Arrays.copyOf(bytes, padding);
    }
    
}
