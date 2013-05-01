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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.util.zip.LoadedZipFile;
import net.derquinse.common.util.zip.MaybeCompressed;
import net.derquinse.common.util.zip.ZipFileLoader;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.InputSupplier;

/**
 * Bocas bucket decorator with zip file support.
 * @author Andres Rodriguez.
 */
@Beta
public final class ZipBocas extends ForwardingBocas {
	/** Delegate. */
	private final Bocas bocas;

	/** Decorates an existing bucket with zip file support. */
	public static ZipBocas of(Bocas bocas) {
		if (bocas instanceof ZipBocas) {
			return (ZipBocas) bocas;
		}
		return new ZipBocas(bocas);
	}

	/** Constructor. */
	private ZipBocas(Bocas bocas) {
		this.bocas = checkNotNull(bocas);
	}

	@Override
	protected Bocas delegate() {
		return bocas;
	}

	private Map<String, ByteString> putZip(LoadedZipFile data) throws IOException {
		if (data == null || data.isEmpty()) {
			return ImmutableMap.of();
		}
		ImmutableMap.Builder<String, ByteString> builder = ImmutableMap.builder();
		List<LoadedBocasValue> values = Lists.newArrayListWithCapacity(data.size());
		for (Entry<String, MemoryByteSource> d : data.entrySet()) {
			LoadedBocasValue value = BocasValue.of(d.getValue().read());
			values.add(value);
			builder.put(d.getKey(), value.key());
		}
		putAll(values);
		return builder.build();
	}

	private Map<String, MaybeCompressed<ByteString>> putZipAndGZip(LoadedZipFile data) throws IOException {
		if (data == null || data.isEmpty()) {
			return ImmutableMap.of();
		}
		ImmutableMap.Builder<String, MaybeCompressed<ByteString>> builder = ImmutableMap.builder();
		List<LoadedBocasValue> values = Lists.newArrayListWithCapacity(data.size());
		for (Entry<String, MaybeCompressed<MemoryByteSource>> d : data.maybeGzip().entrySet()) {
			LoadedBocasValue value = BocasValue.of(d.getValue().getPayload().read());
			boolean compressed = d.getValue().isCompressed();
			values.add(value);
			builder.put(d.getKey(), MaybeCompressed.of(compressed, value.key()));
		}
		putAll(values);
		return builder.build();
	}

	/**
	 * Puts a zip object into the repository.
	 * @return A map from the zip entry names to their keys.
	 * @throws BocasException if an error occurs.
	 */
	public Map<String, ByteString> putZip(InputStream input) {
		try {
			return putZip(ZipFileLoader.get().load(input));
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Puts a zip object into the repository.
	 * @return A map from the zip entry names to their keys.
	 * @throws BocasException if an error occurs.
	 */
	public Map<String, ByteString> putZip(InputSupplier<? extends InputStream> input) {
		try {
			return putZip(ZipFileLoader.get().load(input));
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Puts a zip object into the repository trying to compress them individually with gzip.
	 * @return A map from the zip entry names to their keys, indicating if the entry has been
	 *         compressed.
	 * @throws BocasException if an error occurs.
	 */
	public Map<String, MaybeCompressed<ByteString>> putZipAndGZip(InputStream input) {
		try {
			return putZipAndGZip(ZipFileLoader.get().load(input));
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Puts a zip object into the repository trying to compress them individually with gzip.
	 * @return A map from the zip entry names to their keys, indicating if the entry has been
	 *         compressed.
	 * @throws BocasException if an error occurs.
	 */
	public Map<String, MaybeCompressed<ByteString>> putZipAndGZip(InputSupplier<? extends InputStream> input) {
		try {
			return putZipAndGZip(ZipFileLoader.get().load(input));
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

}
