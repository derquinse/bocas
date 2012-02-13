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

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.util.zip.MaybeCompressed;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.io.InputSupplier;

/**
 * Interface for a BOCAS repository. Guava library is expected to provide a ByteString object. The
 * use of the one from derquinse-commons is temporary.
 * @author Andres Rodriguez.
 */
@Beta
public interface Bocas {
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
	Optional<BocasValue> get(ByteString key);

	/**
	 * Returns the objects for a collection of keys.
	 * @param keys The requested keys.
	 * @return The objects found in the repository or an empty map if none is found.
	 * @throws BocasException if an error occurs.
	 */
	Map<ByteString, BocasValue> get(Iterable<ByteString> keys);

	/**
	 * Puts an object into the repository.
	 * @return The generated key.
	 * @throws BocasException if an error occurs.
	 */
	ByteString put(InputSupplier<? extends InputStream> object);

	/**
	 * Puts an object into the repository.
	 * @return The generated key.
	 * @throws BocasException if an error occurs.
	 */
	ByteString put(InputStream object);

	/**
	 * Puts some objects into the repository in a single operation. The operation must be atomic.
	 * @return The list of generated keys, in the same order than the objects provided.
	 * @throws BocasException if an error occurs.
	 */
	List<ByteString> putSuppliers(List<? extends InputSupplier<? extends InputStream>> objects);

	/**
	 * Puts some objects into the repository in a single operation. The operation must be atomic.
	 * @return The list of generated keys, in the same order than the objects provided.
	 * @throws BocasException if an error occurs.
	 */
	List<ByteString> putStreams(List<? extends InputStream> objects);

	/**
	 * Puts a zip object into the repository.
	 * @return A map from the zip entry names to their keys.
	 * @throws BocasException if an error occurs.
	 */
	Map<String, ByteString> putZip(InputStream object);

	/**
	 * Puts a zip object into the repository.
	 * @return A map from the zip entry names to their keys.
	 * @throws BocasException if an error occurs.
	 */
	Map<String, ByteString> putZip(InputSupplier<? extends InputStream> object);

	/**
	 * Puts a zip object into the repository trying to compress them individually with gzip.
	 * @return A map from the zip entry names to their keys, indicating if the entry has been
	 *         compressed.
	 * @throws BocasException if an error occurs.
	 */
	Map<String, MaybeCompressed<ByteString>> putZipAndGZip(InputStream object);

	/**
	 * Puts a zip object into the repository trying to compress them individually with gzip.
	 * @return A map from the zip entry names to their keys, indicating if the entry has been
	 *         compressed.
	 * @throws BocasException if an error occurs.
	 */
	Map<String, MaybeCompressed<ByteString>> putZipAndGZip(InputSupplier<? extends InputStream> object);

}
