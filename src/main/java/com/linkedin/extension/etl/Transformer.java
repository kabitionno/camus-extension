package com.linkedin.extension.etl;
public interface Transformer<S, R> {
	R transform(S s);
}
