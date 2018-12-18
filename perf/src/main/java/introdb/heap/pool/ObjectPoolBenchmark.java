package introdb.heap.pool;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantLock;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class ObjectPoolBenchmark {

	@Param({"25"})
	public int poolSize;
		
	private ObjectPool<Object> pool;

	@Setup(Level.Iteration)
	public void setUp() throws IOException, ClassNotFoundException {
		pool = new ObjectPool<>(Object::new, o -> true, poolSize);
	}

	@TearDown(Level.Iteration)
	public void tearDown() throws IOException, InterruptedException {
		pool.shutdown();
	}

	@Benchmark
	@Threads(8)
	public void testPool(Blackhole blackhole) throws InterruptedException, ExecutionException {
		CompletableFuture<Object> future = pool.borrowObject();
		Object obj = future.get();
		try {
			blackhole.consume(obj);
		} finally {
			pool.returnObject(obj);
		}
	}

}
