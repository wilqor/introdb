package introdb.heap.pool;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ObjectPool<T> {

	private final ConcurrentLinkedDeque<T> objectPool = new ConcurrentLinkedDeque<>();	
	private final ConcurrentLinkedDeque<CompletableFuture<T>> borrowObjectTasks = new ConcurrentLinkedDeque<>();
	private final ExecutorService waitingTasks = Executors.newFixedThreadPool(64);

	private final AtomicLong poolState = new AtomicLong(0);

	private final ObjectFactory<T> fcty;
	private final ObjectValidator<T> validator;
	private int maxPoolSize = 25;

	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
		this.fcty = fcty;
		this.validator = validator;
	}
	
	public CompletableFuture<T> borrowObject() {

		// fast path, in case there is object in pool, return it immediately
		T object = tryToBorrow();
		if (object != null) {
			return completedFuture(object);
		}

		// check if there is still free place in pool
		boolean growPool = tryToGrow();

		// grow the pool, if necessary
		if (growPool) {
			return completedFuture(fcty.create());
		}

		// all objects are in use, schedule task
		CompletableFuture<T> future = new CompletableFuture<>();
		borrowObjectTasks.offer(future);
		return future;
	}	
	
	public void returnObject(T object) {
		if (validator.validate(object)) {
			// piggyback, on release check if there is any task waiting for object
			CompletableFuture<T> future = borrowObjectTasks.poll();
			if (future != null) {
				future.complete(object);
			} else {
				objectPool.offer(object);
				long currentState;
				long newState;
				do {
					currentState = poolState.get();
					int inUse = (int) (currentState >> 32);
					int inPool = (int) currentState;
					inUse--;
					newState = (long) inUse << 32 | inPool & 0xFFFFFFFFL;
				} while (currentState != poolState.getAndSet(newState));

			}
		}
	}

	public void shutdown() throws InterruptedException {
		waitingTasks.shutdown();
		waitingTasks.awaitTermination(10, TimeUnit.SECONDS);
	}

	private T tryToBorrow() {
		T object = objectPool.poll();
		if (object != null) {
			long newState = 0;
			long currentState = 0;
			do {
				currentState = poolState.get();
				int inUse = (int) (currentState >> 32);
				int inPool = (int) currentState;
				inUse++;
				newState = (long) inUse << 32 | inPool & 0xFFFFFFFFL;
			} while (currentState != poolState.getAndSet(newState));
		}
		return object;
	}
	
	private boolean tryToGrow() {
		long newState;
		long currentState;
		boolean growPool;
		do {
			growPool = false;
			currentState = poolState.get();
			int inUse = (int) (currentState >> 32);
			int inPool = (int) currentState;
			if (inPool < maxPoolSize) {
				growPool = true;
				inPool++;
				inUse++;
			}
			newState = (long) inUse << 32 | inPool & 0xFFFFFFFFL;
		} while (currentState != poolState.getAndSet(newState));
		return growPool;
	}

}
