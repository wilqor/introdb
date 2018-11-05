package introdb.heap.pool;

public interface ObjectValidator<T> {
		
	boolean preRelease(T object);

}
