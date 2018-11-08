package introdb.heap;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConcurrentReadWriteUnorderedHeapFileTest {

	private Path heapFilePath;
	private Store heapFile;

	@BeforeEach
	public void setUp() throws IOException {
		heapFilePath = Files.createTempFile("heap", "0001");
		heapFile = new UnorderedHeapFile(heapFilePath, 1024, 4 * 1024);
	}

	@AfterEach
	public void tearDown() throws IOException {
		Files.delete(heapFilePath);
	}

	@Test
	public void concurrentReadWrite() {

		ExecutorService readersExecutor = Executors.newCachedThreadPool();
		ExecutorService writers = Executors.newCachedThreadPool();

		int nrOfReaders = 5;
		List<CompletableFuture<Integer>> rs = IntStream.range(0, nrOfReaders).mapToObj(readerOf(readersExecutor))
		        .collect(toList());

		int nrOfWriters = 5;
		List<CompletableFuture<Integer>> ws = IntStream.range(0, nrOfWriters).mapToObj(writerOf(writers))
		        .collect(toList());

		CompletableFuture.allOf(rs.toArray(new CompletableFuture[rs.size()]));

		CompletableFuture.allOf(ws.toArray(new CompletableFuture[ws.size()]));
	}

	private IntFunction<CompletableFuture<Integer>> readerOf(ExecutorService executorService) {
		return (i) -> {
			CompletableFuture<Integer> cf = new CompletableFuture<>();
			Future<Integer> future = executorService.submit(this::doReads, i);
			cf.completeAsync(uncheckedFuture(future));
			return cf;
		};
	}

	private IntFunction<CompletableFuture<Integer>> writerOf(ExecutorService writers) {
		return (i) -> {
			CompletableFuture<Integer> cf = new CompletableFuture<>();
			Future<Integer> future = writers.submit(this::doWrites, i);
			cf.completeAsync(uncheckedFuture(future));
			return cf;
		};
	}

	private void doReads() {
		Random random = new Random();
		int nextInt = random.nextInt(50);
		for (int i = 0; i < 1000; i++) {
			try {
				Integer value = (Integer) heapFile.get(nextInt);

				if (value != null && value.equals(Integer.valueOf(nextInt))) {
					fail("incorrect value");
				}
			} catch (ClassNotFoundException | IOException e) {
				fail(e);
			}
		}
	}

	private void doWrites() {
		Random random = new Random();
		int nextInt = random.nextInt(50);
		for (int i = 0; i < 1000; i++) {
			try {
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
