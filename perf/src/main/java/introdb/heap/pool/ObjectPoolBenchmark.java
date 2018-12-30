package introdb.heap.pool;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@State(Scope.Benchmark)
public class ObjectPoolBenchmark {

	@Param({"25"})
	public int poolSize;
		
	private ObjectPool<Object> pool;

	@Setup(Level.Iteration)
	public void setUp() {
		pool = new ObjectPool<>(Object::new, o -> true, poolSize);
	}

	@TearDown(Level.Iteration)
	public void tearDown() {
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
