package introdb.heap.pool;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Pool<T> {

	private final ConcurrentLinkedDeque<T> deque = new ConcurrentLinkedDeque<>();
	private final ObjectFactory<T> fcty;
	private final ObjectValidator<T> validator;
	
	public Pool(ObjectFactory<T> fcty, ObjectValidator<T> validator) {
		this.fcty = fcty;
		this.validator = validator;
	}
	
	public T get() {
		T object = deque.poll();
		if(object==null) {
			object = fcty.create();
		}
		return object;
	}
	
	public void release(T object) {
		if(validator.preRelease(object)){
			deque.offer(object);
		}
	}
	
	public static void main(String[] args) {
		Pool<ReentrantReadWriteLock> pool = new Pool<>(()-> new ReentrantReadWriteLock(), t -> !t.isWriteLocked() && t.getReadLockCount()==0);
		ReentrantReadWriteLock lock = pool.get();
		System.out.println(lock);
		pool.release(lock);
		System.out.println(pool.get());
	}
	
	
}
