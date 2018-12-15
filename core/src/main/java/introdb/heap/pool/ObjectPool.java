package introdb.heap.pool;

import java.util.concurrent.CompletableFuture;

public class ObjectPool<T> {

	private final ObjectFactory<T> fcty;
	private final ObjectValidator<T> validator;
	private final int maxPoolSize;

	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
		this(fcty,validator,25);
	}
	
	public ObjectPool(ObjectFactory<T> fcty, ObjectValidator<T> validator, int maxPoolSize) {
		this.fcty = fcty;
		this.validator = validator;
		this.maxPoolSize = maxPoolSize;
	}
	
	/**
	 * When there is object in pool returns completed future,
	 * if not, future will be completed when object is
	 * returned to the pool.
	 * 
	 * @return
	 */
	public CompletableFuture<T> borrowObject() {
		return new CompletableFuture<T>();
	}	
	
	public void returnObject(T object) {
	}

	public void shutdown() throws InterruptedException {
	}

	public int getPoolSize() {
		return 0;
	}

	public int getInUse() {
		return 0;
	}

}
