package introdb.heap.lock;

import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import introdb.heap.lock.LockManager.Lock;

public class LockManagerTest {
	
	private LockManager lockManager;

	@BeforeEach
	public void setUp() {
		lockManager = new LockManager();
	}
	
	@AfterEach
	public void tearDown() throws InterruptedException {
		lockManager.stop();
	}
	
	@Test
	public void a() throws InterruptedException, ExecutionException {
		
		Lock lock0 = lockManager.lockForPage(0);
		ReadLock readLock0 = lock0.readLock();
		
		Lock lock1 = lockManager.lockForPage(1);
		ReadLock readLock1 = lock1.readLock();
		
		assertNotEquals(readLock0, readLock1);
	}

	@Test
	public void b() throws InterruptedException, ExecutionException {
		
		Lock lock0 = lockManager.lockForPage(0);
		ReadLock readLock0 = lock0.readLock();
		
		Lock lock1 = lockManager.lockForPage(0);
		ReadLock readLock1 = lock1.readLock();
		
		assertEquals(readLock0, readLock1);
	}

	@Test
	public void c() throws InterruptedException, ExecutionException {
		
		Lock lock0 = lockManager.lockForPage(0);
		String lock0ToString = lock0.toString();
		
		lock0 = null; // lock0 is now unreachable
		
		System.gc(); // force WeakRef processing
		
		Lock lock1 = lockManager.lockForPage(0);
		
		assertNotEquals(lock0ToString, lock1.toString());
	}
}
