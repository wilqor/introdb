package introdb.heap.pool;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectPool<T> {

	private final ConcurrentLinkedQueue<T> objectPool = new ConcurrentLinkedQueue<>();
	private final ConcurrentLinkedQueue<CompletableFuture<T>> borrowObjectTasks = new ConcurrentLinkedQueue<>();
	private final ExecutorService waitingTasks = Executors
	        .newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	private final AtomicInteger poolSize = new AtomicInteger(0);

	private final ObjectFactory<T> fcty;
	private final ObjectValidator<T> validator;
	private final int maxPoolSize;

	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
		this(fcty, validator, 25);
	}

	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
		this.fcty = fcty;
		this.validator = validator;
		this.maxPoolSize = maxPoolSize;
	}

	/**
	 * When there is object in pool returns completed future, if not, future will be
	 * completed when object is returned to the pool.
	 * 
	 * @return
	 */
	public CompletableFuture<T> borrowObject() {

		// fast path, in case there is object in pool, return it immediately
		T object = objectPool.poll();
		if (object != null) {
			return completedFuture(object);
		}

		if (poolSize.get() == maxPoolSize) {
			return uncompletedRequest();
		}

		int claimed;
		int next;
		do {
			claimed = poolSize.get();
			next = claimed + 1;
			if (next > maxPoolSize) { // when competing thread reached max first, wait
				return uncompletedRequest();
			}
		} while (claimed!=poolSize.compareAndExchangeRelease(claimed, next));

		object = fcty.create();

		return completedFuture(object);
	}

	public void returnObject(T object) {
		if (validator.validate(object)) {
			// piggyback, on release check if there is any task waiting for object
			CompletableFuture<T> future = borrowObjectTasks.poll();
			if (future != null) {
				future.complete(object);
			} else {
				objectPool.offer(object);
			}
		}
	}

	public void shutdown() throws InterruptedException {
		waitingTasks.shutdown();
		waitingTasks.awaitTermination(10, TimeUnit.SECONDS);
	}

	public int getPoolSize() {
		long currentState = poolSize.get();
		return (int) currentState;
	}

	public int getInUse() {
		return poolSize.get() - objectPool.size();
	}

	private CompletableFuture<T> uncompletedRequest() {
		var req = new CompletableFuture<T>();
		borrowObjectTasks.add(req);
		return req;
	}

}
