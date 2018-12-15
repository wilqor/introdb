package introdb.heap.pool;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ObjectPoolTest {

	private ObjectPool<Object> objectPool;

	@AfterEach
	public void tearDown() throws Exception {
		objectPool.shutdown();
	}

	@Test
	void return_distinct_objects_from_pool() throws Exception {
		objectPool = new ObjectPool<>(Object::new, obj -> true);

		assertEquals(0, objectPool.getPoolSize());
		assertEquals(0, objectPool.getInUse());

		var first = objectPool.borrowObject();
		var second = objectPool.borrowObject();

		assertNotEquals(first.get(1, TimeUnit.SECONDS), second.get(1, TimeUnit.SECONDS));
		assertEquals(2, objectPool.getPoolSize());
		assertEquals(2, objectPool.getInUse());
		
	}

	@Test
	void return_same_object_after_returned_to_pool() throws Exception {
		objectPool = new ObjectPool<>(Object::new, obj -> true);

		var first = objectPool.borrowObject();

		objectPool.returnObject(first.get(1, TimeUnit.SECONDS));

		var second = objectPool.borrowObject();

		assertEquals(first.get(), second.get(1, TimeUnit.SECONDS));
		assertEquals(1, objectPool.getPoolSize());
		assertEquals(1, objectPool.getInUse());
	}

	@Test
	void return_uncompleted_future_when_out_of_objects() throws Exception {
		objectPool = new ObjectPool<>(Object::new, obj -> true, 1);

		var first = objectPool.borrowObject();
		var second = objectPool.borrowObject();

		assertNotNull(first.get(1, TimeUnit.SECONDS));
		assertFalse(second.isDone());

		objectPool.returnObject(first.get());

		assertEquals(first.get(), second.get(1, TimeUnit.SECONDS));
	}
}
