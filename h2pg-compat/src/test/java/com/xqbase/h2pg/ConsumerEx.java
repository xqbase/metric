package com.xqbase.h2pg;

@FunctionalInterface
public interface ConsumerEx<T, E extends Exception> {
	public void accept(T t) throws E;

	public default ConsumerEx<T, E> andThen(ConsumerEx<? super T, ? extends E> after) {
		return t -> {
			accept(t);
			after.accept(t);
		};
	}
}