package introdb.heap;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.Spliterator.OfInt;
import java.util.stream.IntStream;

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
		
	private static final int MAX_PAGES = 100_000;
	private static final int MAX_RANDOM_KEYS = 100_000_000;
	private static final Random KeyGenerator = new Random();
	
	@Param( {"512","1024","2048"})
	public int bufferSize; 
	private Store heapFile;
	private int nextWriteKey;
	private Path tempFile;
	private int[] randomKeys;
	private OfInt ofInt;
	
	@Setup(Level.Iteration)
	public void setUp() throws IOException, ClassNotFoundException {
		
		nextWriteKey = 0;
		
		// we want to have repeatable results, thus we prepopulate array of random keys, and store it to be used
		// during whole run of benchmark (even if we have more than one fork)
		Path randomKeysPath = Paths.get(System.getProperty("java.io.tmpdir"), getClass().getName()+"-randomKeys");
		
		if(Files.exists(randomKeysPath)) {
			try(var input = new ObjectInputStream(Files.newInputStream(randomKeysPath, StandardOpenOption.READ))) {
				randomKeys = (int[])input.readObject();
			}
		} else {
			randomKeys = new int[MAX_RANDOM_KEYS];
			IntStream.range(0, MAX_RANDOM_KEYS).forEach( i-> randomKeys[i]=KeyGenerator.nextInt(MAX_PAGES));
			try(var output = new ObjectOutputStream(Files.newOutputStream(randomKeysPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.TRUNCATE_EXISTING , StandardOpenOption.WRITE))){
				output.writeObject(randomKeys);
			}
		}
		
		ofInt = Arrays.spliterator(randomKeys);
		
		tempFile = Files.createTempFile("heap", "0001");
		heapFile = new UnorderedHeapFile(tempFile, MAX_PAGES, 4*1024);
	}
	
	@TearDown(Level.Iteration)
	public void tearDown() throws IOException{
		Files.delete(tempFile);
	}
	
    @Benchmark
    @Group("concurrent_read_write")
    public int writeEntry() throws ClassNotFoundException, IOException {
    	int newKey = nextWriteKey++;
    	heapFile.put(new Entry(toArrayWithPadding(newKey, 64), toArrayWithPadding(newKey, bufferSize)));
    	return newKey;
    }

    @Benchmark
    @Group("concurrent_read_write")
    public boolean readEntry(Blackhole blackhole) {
    	
    	return ofInt.tryAdvance( (int i)-> {
			try {
				byte[] bytes = (byte[]) heapFile.get(i);
				blackhole.consume(bytes);
				if(bytes!=null && !Arrays.equals(bytes, toArrayWithPadding(i, bufferSize))) {
					throw new IllegalStateException("heap file is corrupted");
				}    		
			} catch (ClassNotFoundException | IOException e) {
				throw new RuntimeException(e);
			}
    	});
    	
    }
    
    byte[] toArrayWithPadding(int value, int padding) {
    	byte[] bytes = Integer.toString(value).getBytes();
    	return Arrays.copyOf(bytes, padding);
    }
    
}
