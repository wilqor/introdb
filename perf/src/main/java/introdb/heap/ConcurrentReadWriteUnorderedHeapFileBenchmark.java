package introdb.heap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class ConcurrentReadWriteUnorderedHeapFileBenchmark {
		
	private static final int MAX_PAGES = 50000;
	@Param( {"512","1024","2048"})
	public int bufferSize; 
	private Store heapFile;
	private int key;
	private Path tempFile;
	private Random keyGen;
	
	@Setup(Level.Iteration)
	public void setUp() throws IOException {
		tempFile = Files.createTempFile("heap", "0001");
		heapFile = new UnorderedHeapFile(tempFile, MAX_PAGES, 4*1024);
		key = 0;
		keyGen = new Random();
	}
	
	@TearDown(Level.Iteration)
	public void tearDown() throws IOException{
		Files.delete(tempFile);
	}
	
    @Benchmark
    @Group("concurrent_read_write")
    public void writeEntry() throws ClassNotFoundException, IOException {
    	int newKey = key++;
    	heapFile.put(new Entry(toArrayWithPadding(newKey, 64), toArrayWithPadding(newKey, bufferSize)));
    }

    @Benchmark
    @Group("concurrent_read_write")
    public Object readEntry() throws ClassNotFoundException, IOException {
    	int newKey = keyGen.nextInt(MAX_PAGES);
		byte[] bytes = (byte[]) heapFile.get(newKey);
		if(bytes!=null && !Arrays.equals(bytes, toArrayWithPadding(newKey, bufferSize))) {
			throw new IllegalStateException("heap file is corrupted");
		}
		return bytes;
    }
    
    byte[] toArrayWithPadding(int value, int padding) {
    	byte[] bytes = Integer.toString(value).getBytes();
    	return Arrays.copyOf(bytes, padding);
    }
    
    
}
