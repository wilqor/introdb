package introdb.heap.pool;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.runAsync;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Pool<T> {

	private final ConcurrentLinkedDeque<T> pool = new ConcurrentLinkedDeque<>();
	
	private final ConcurrentLinkedDeque<CompletableFuture<T>> tasks = new ConcurrentLinkedDeque<>();

	private final ObjectFactory<T> fcty;
	private final ObjectValidator<T> validator;
	private final ExecutorService s = Executors.newFixedThreadPool(64);

	private AtomicLong poolState = new AtomicLong(0);
	private int maxPoolSize = 25;

	public Pool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
		this.fcty = fcty;
		this.validator = validator;
	}
	
	private T tryToBorrow() {
		T object = pool.poll();
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

	public CompletableFuture<T> get() {

		// fast path, in case there is object in pool, return it immediately
		T object = tryToBorrow();
		if (object != null) {
			return completedFuture(object);
		}

		// check if there is still free place in pool
		boolean growPool = tryToGrow();

		// grow the pool
		if (growPool) {
			return runAsync(() -> {
				pool.offer(fcty.create());
			}, s).thenCompose(f -> {
				// work stealing? somebody can steal this object, but it's fine
				return get();
			});
		}

		// all objects are in use, schedule task
		CompletableFuture<T> future = new CompletableFuture<>();
		tasks.offer(future);
		return future;
	}	
	
	public void shutdown() throws InterruptedException {
		s.shutdown();
		s.awaitTermination(10, TimeUnit.SECONDS);
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
			}
			newState = (long) inUse << 32 | inPool & 0xFFFFFFFFL;
		} while (currentState != poolState.getAndSet(newState));
		return growPool;
	}

	public void release(T object) {
		if (validator.preRelease(object)) {
			// piggyback, on release check if there is any task waiting for object
			CompletableFuture<T> future = tasks.poll();
			if (future != null) {
				future.complete(object);
			} else {
				pool.offer(object);
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

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Pool<ReentrantReadWriteLock> pool = new Pool<>(ReentrantReadWriteLock::new, t -> !t.isWriteLocked() && t.getReadLockCount() == 0);
		
		ExecutorService threadPool = Executors.newCachedThreadPool();
		List<Future<?>> futures = new ArrayList<>();
		for (int i = 0; i < 30; i++) {
			Future<?> f = threadPool.submit(() -> {
				for (int j = 0; j < 1000; j++) {
					CompletableFuture<ReentrantReadWriteLock> future = pool.get();
					ReentrantReadWriteLock lock = null;
					try {
						lock = future.get();
						lock.readLock().lock();
						Thread.sleep(10);
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					} finally {
						lock.readLock().unlock();
						pool.release(lock);
					}
				}
			});
			
			futures.add(f);
		}
		
		while(!futures.isEmpty()) {
			Future<?> future = futures.get(0);
			future.get();
			System.out.println(future);
			futures.remove(future);
		}
		
		System.out.println("I am done");
		
		threadPool.shutdown();
		threadPool.awaitTermination(10, TimeUnit.SECONDS);
		

	}

}
