package introdb.heap.pool;

public interface ObjectValidator<T> {
		
	boolean validate(T object);

}
