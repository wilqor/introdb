package introdb.heap;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import introdb.heap.pool.ObjectPool;


public class LockManager {
	
	class LockRef {
		final AtomicLong refCount = new AtomicLong();
		final CompletableFuture<ReentrantReadWriteLock> futureLock;
		LockRef(CompletableFuture<ReentrantReadWriteLock> futureLock){
			this.futureLock = futureLock;
		}
	}
	
	private final ConcurrentHashMap<Integer, LockRef> locks = new ConcurrentHashMap<>();
	private final ObjectPool<ReentrantReadWriteLock> objectPool = new ObjectPool<>(ReentrantReadWriteLock::new, l -> l.getWriteHoldCount()==0 && l.getReadHoldCount()==0);
	
	public void readLock(Integer pageNr) throws InterruptedException, ExecutionException {
		var futureLock = locks.computeIfAbsent(pageNr, p -> new LockRef(objectPool.borrowObject()));
		ReentrantReadWriteLock lock = futureLock.futureLock.get();
		futureLock.refCount.incrementAndGet();
		lock.readLock().lock();
	}
	
	public void readUnlock(Integer pageNr) throws InterruptedException, ExecutionException {
		var futureLock = locks.get(pageNr);
		ReentrantReadWriteLock lock = futureLock.futureLock.get();
		lock.readLock().unlock();
		futureLock.refCount.decrementAndGet();
		futureLock.refCount.
	}

}
