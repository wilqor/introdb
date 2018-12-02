package introdb.heap.lock;

import static java.lang.String.format;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import introdb.heap.pool.ObjectPool;

public class LockManager {

	private static final Logger LOGGER = Logger.getLogger(LockManager.class.getName());
	
	/**
	 * This is a neat trick, as {@link WeakReference} docs suggests 
	 * we need canonical mapping of ReentrantLock.
	 * 
	 */
	class Lock {

		private final CompletableFuture<ReentrantReadWriteLock> futureLock;

		Lock(CompletableFuture<ReentrantReadWriteLock> futureLock) {
			this.futureLock = futureLock;
		}
		
		public ReadLock readLock() throws InterruptedException, ExecutionException {
			return futureLock.get().readLock(); // actually we should not let ReadLock leak, as this may lead to weird problems, 
		}
	}

	/**
	 * Holds reentrant lock so we can return it to the pool when {@link Lock} is
	 * garbage collected.
	 *
	 */
	class LockRef extends WeakReference<Lock> {

		private final CompletableFuture<ReentrantReadWriteLock> futureLock;
		private final Integer pageNr;

		LockRef(Integer pageNr, 
				Lock referent, 
				CompletableFuture<ReentrantReadWriteLock> futureLock,
		        ReferenceQueue<Lock> q) {
			super(referent, q);
			this.pageNr = pageNr;
			this.futureLock = futureLock;
		}

		@Override
		public void clear() {
			super.clear();
			boolean removed = locks.remove(pageNr, this);
			LOGGER.info(() -> format("lock for page %d was %s removed", pageNr, removed?"":"not"));
			try {
				if (futureLock.isDone() && (!futureLock.isCancelled() && !futureLock.isCompletedExceptionally()))
					objectPool.returnObject(futureLock.get());
			} catch (InterruptedException | ExecutionException e) {
				LOGGER.log(Level.SEVERE,format("failed to clear lock ref %s",this),e);
			}
		}
	}

	private final ConcurrentHashMap<Integer, LockRef> locks = new ConcurrentHashMap<>();
	private final ObjectPool<ReentrantReadWriteLock> objectPool = new ObjectPool<>(ReentrantReadWriteLock::new,l -> l.getWriteHoldCount() == 0 && l.getReadHoldCount() == 0);
	private final ReferenceQueue<Lock> referenceQ = new ReferenceQueue<>();
	private final ExecutorService invalidator = Executors.newSingleThreadExecutor();
	private volatile boolean running = true;
	
	public LockManager() {
		invalidator.execute(() -> {
			while (running) {
				try {
					Reference<? extends Lock> reference = referenceQ.remove(1000);
					if(reference!=null) {
						LOGGER.info(()->format("processing reference %s", reference));	
						reference.clear();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	public void stop() throws InterruptedException {
		running = false;
		invalidator.shutdown();
		invalidator.awaitTermination(2000, TimeUnit.MILLISECONDS);
	}

	public Lock lockForPage(Integer pageNr) throws InterruptedException, ExecutionException {
		var futureLock = locks.compute(pageNr, (_pageNr,_lockRef) -> {
			// handling situation when there is no mapping, or mapping points to unreachable lock
			// instance, which was not yet processed by invalidator
			if(_lockRef==null || _lockRef.get()==null) {
				CompletableFuture<ReentrantReadWriteLock> future = objectPool.borrowObject();
				return new LockRef(_pageNr, new Lock(future), future, referenceQ);
			} else {
				return _lockRef;
			}
		});
		return futureLock.get();
	}

}
