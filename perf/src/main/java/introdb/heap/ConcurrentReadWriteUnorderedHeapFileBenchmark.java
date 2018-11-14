package introdb.heap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.CompilerControl.Mode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

@State(Scope.Benchmark)
public class ConcurrentReadWriteUnorderedHeapFileBenchmark {

	private static final int MAX_PAGES = 100_000;
	
	@Param( {"512","1024","2048"})
	public int bufferSize;
	
	private byte[] readKey_0 = toArrayWithPadding(0, 64);

	private byte[] writeKey_1 = toArrayWithPadding(1, 64);
	private byte[] writeValue_1;
	
	private UnorderedHeapFile heapFile;
	private Path tempFile;
	
	@Setup(Level.Iteration)
	public void setUp() throws IOException, ClassNotFoundException, InterruptedException {
						
		Process sync = new ProcessBuilder("sync").start();
		if(sync.waitFor()!=0) {
			throw new IllegalStateException("sync command failed");
		}
		
		Process sysctl= new ProcessBuilder("sysctl","-w","vm.drop_caches=1").start();
		if(sysctl.waitFor()!=0) {
			throw new IllegalStateException("sysctl command failed");			
		}

		tempFile = Files.createTempFile("heap", "0001");
		heapFile = new UnorderedHeapFile(tempFile, MAX_PAGES, 4*1024);

		heapFile.put(new Entry(readKey_0, toArrayWithPadding(0, bufferSize)));

		writeValue_1 = toArrayWithPadding(1, bufferSize);
	}
	
	@TearDown(Level.Iteration)
	public void tearDown() throws IOException{
		Files.delete(tempFile);
	}
	
    @Benchmark
    @Group("concurrent_read_write")
    @CompilerControl(Mode.EXCLUDE)
    public Entry writeEntry() throws ClassNotFoundException, IOException {
    	Entry entry = new Entry(writeKey_1, writeValue_1);
		heapFile.put(entry);
		return entry;
    }

    @Benchmark
    @Group("concurrent_read_write")
    @CompilerControl(Mode.EXCLUDE)
    public byte[] readEntry() throws ClassNotFoundException, IOException {
		return (byte[]) heapFile.get(readKey_0);
	}
    
    private static byte[] toArrayWithPadding(int value, int padding) {
    	byte[] bytes = Integer.toString(value).getBytes();
    	return Arrays.copyOf(bytes, padding);
    }
    
}
