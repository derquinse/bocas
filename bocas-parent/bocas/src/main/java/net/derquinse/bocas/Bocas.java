/*
 * Copyright (C) the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.derquinse.bocas;

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.io.ByteSource;

/**
 * Interface for a Bocas repository bucket.
 * @author Andres Rodriguez.
 */
@Beta
public interface Bocas {
	/** Returns the hash function being used. */
	BocasHashFunction getHashFunction();

	/**
	 * Closes the bucket so it cannot be used again. Optional operation, buckets that do not need to
	 * be closed ignore this operation. Any operation other than close should throw
	 * {@link IllegalStateException}. Calling close in a closed bucket is a no-op.
	 */
	void close();

	/**
	 * Returns whether the repository contains the provided key.
	 * @throws BocasException if an error occurs.
	 */
	boolean contains(ByteString key);

	/**
	 * Returns the subset of the provided keys that are contained in the repository.
	 * @throws BocasException if an error occurs.
	 */
	Set<ByteString> contained(Iterable<ByteString> keys);

	/**
	 * Returns the object for the provided key.
	 * @param key The requested key.
	 * @return The object, if found.
	 * @throws BocasException if an error occurs.
	 */
	Optional<ByteSource> get(ByteString key);

	/**
	 * Returns the objects for a collection of keys.
	 * @param keys The requested keys.
	 * @return The objects found in the repository or an empty map if none is found.
	 * @throws BocasException if an error occurs.
	 */
	Map<ByteString, ByteSource> get(Iterable<ByteString> keys);

	/**
	 * Puts a value into the repository.
	 * @return The generated key.
	 * @throws BocasException if an error occurs.
	 */
	ByteString put(ByteSource value);

	/**
	 * Puts some values into the repository in a single operation. The operation must be atomic.
	 * @return The list of generated keys, in the same order than the objects provided.
	 * @throws BocasException if an error occurs.
	 */
	List<ByteString> putAll(Iterable<? extends ByteSource> values);
}
