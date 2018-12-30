package introdb.heap.pool;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

class ObjectPool<T> {

	private final ObjectFactory<T> factory;
	private final ObjectValidator<T> validator;
	private final int maxPoolSize;

	private final ArrayBlockingQueue<T> objectPool;
	private final ConcurrentLinkedQueue<CompletableFuture<T>> borrowObjectTasks = new ConcurrentLinkedQueue<>();

	private final AtomicInteger poolSize = new AtomicInteger(0);

	ObjectPool(ObjectFactory<T> factory, ObjectValidator<T> validator) {
		this(factory, validator, 25);
	}

	ObjectPool(ObjectFactory<T> factory, ObjectValidator<T> validator, int maxPoolSize) {
		this.factory = factory;
		this.validator = validator;
		this.maxPoolSize = maxPoolSize;
		this.objectPool = new ArrayBlockingQueue<>(maxPoolSize);
	}

	/**
	 * When there is object in pool returns completed future, if not, future will be
	 * completed when object is returned to the pool.
	 * 
	 * @return completed future when object in pool, else uncompleted future
	 */
	CompletableFuture<T> borrowObject() {

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
		} while (!poolSize.compareAndSet(claimed, next));

		return completedFuture(factory.create());
	}

	void returnObject(T object) {
		if (validator.validate(object)) {
			// piggyback, on release, check if there is any task waiting for object
			CompletableFuture<T> future = borrowObjectTasks.poll();
			if (future != null) {
				future.complete(object);
			} else {
				objectPool.offer(object);
			}
		}
	}

	int getPoolSize() {
		return poolSize.get();
	}

	int getInUse() {
		return poolSize.get() - objectPool.size();
	}

	private CompletableFuture<T> uncompletedRequest() {
		var req = new CompletableFuture<T>();
		borrowObjectTasks.add(req);
		return req;
	}

}
