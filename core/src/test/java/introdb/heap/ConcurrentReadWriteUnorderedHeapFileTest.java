package introdb.heap;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ConcurrentReadWriteUnorderedHeapFileTest {

	private static final Logger LOG = Logger.getLogger("test.readwrite.concurrent");

	private static final int PAGE_SIZE = 4 * 1024;
	private static final int MAX_NR_PAGES = 5_000_000;
	private static final int OPS_PER_WORKER = 1_000;
	
	private int nrOfWriters = 5;
	private int nrOfReaders = 5;

	private Path heapFilePath;
	private Store heapFile;
	private CountDownLatch writersLatch;
	private ExecutorService executors;

	@BeforeEach
	public void setUp() throws IOException {
		heapFilePath = Files.createTempFile("heap", "0001");
		heapFile = new UnorderedHeapFile(heapFilePath, MAX_NR_PAGES, PAGE_SIZE);

		executors = Executors.newCachedThreadPool();
	}

	@AfterEach
	public void tearDown() throws IOException, InterruptedException {
		Files.delete(heapFilePath);
		
		executors.shutdown();
		executors.awaitTermination(1, TimeUnit.MINUTES);
		
	}

	@Test
	@Tag("slow")
	public void concurrentReadWrite() throws Exception {

		writersLatch = new CountDownLatch(nrOfWriters);		
		
		// schedule threads which execute writes to heap file
		var ws = range(0, nrOfWriters)
				.mapToObj(writersTasksWith(executors))
				.collect(toList());

		// schedule threads which execute reads to heap file
		var rs = range(0, nrOfReaders)
				.mapToObj(readersTasksWith(executors))
				.collect(toList());

		// wait for readers to complete
		CompletableFuture.allOf(ws.toArray(new CompletableFuture[ws.size()])).get(5,TimeUnit.MINUTES);

		// wait for writers to complete
		CompletableFuture.allOf(rs.toArray(new CompletableFuture[rs.size()])).get(5,TimeUnit.MINUTES);
		
	}

	private IntFunction<CompletableFuture<Void>> readersTasksWith(ExecutorService executorService) {
		return (i) -> {
			return new CompletableFuture<>()
					.completeAsync(uncheckedFuture(executorService.submit(this::doReads, i)))
					.thenAccept( r-> LOG.info(format("reader %d is done",r)));
		};
	}

	private IntFunction<CompletableFuture<Void>> writersTasksWith(ExecutorService writers) {
		return (i) -> {
			return new CompletableFuture<>()
					.completeAsync(uncheckedFuture(writers.submit(this::doWrites, i)))
					.thenAccept( r-> LOG.info(format("writer %d is done",r)));
		};
	}

	private void doReads() {
		try {
			// waiting for writers to start
			writersLatch.await();
		} catch (InterruptedException e) {
			fail(e);
		}
		
		Random random = new Random();
		for (int i = 0; i < OPS_PER_WORKER; i++) {
			try {
				int nextInt = random.nextInt(20);
				Integer value = (Integer) heapFile.get(nextInt);
				if (value != null && !value.equals(Integer.valueOf(nextInt))) {
					fail("incorrect value read from heap file");
				}
			} catch (ClassNotFoundException | IOException e) {
				fail(e);
			}
		}
	}

	private void doWrites() {
		writersLatch.countDown();
		Random random = new Random();
		for (int i = 0; i < OPS_PER_WORKER; i++) {
			try {
				int nextInt = random.nextInt(20);
				heapFile.put(new Entry(nextInt, nextInt));
			} catch (ClassNotFoundException | IOException e) {
				fail(e);
			}
		}
	}

	public <T> Supplier<T> uncheckedFuture(Future<T> future) {
		return () -> {
			try {
				return future.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		};
	}
}
