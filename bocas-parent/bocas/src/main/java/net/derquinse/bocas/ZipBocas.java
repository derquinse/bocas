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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.util.zip.LoadedZipFile;
import net.derquinse.common.util.zip.MaybeCompressed;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

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

	/**
	 * Puts a loaded zip file into the repository.
	 * @return A map from the zip entry names to their keys.
	 * @throws BocasException if an error occurs.
	 */
	public Map<String, ByteString> putZip(LoadedZipFile data) {
		if (data == null || data.isEmpty()) {
			return ImmutableMap.of();
		}
		final int n = data.size();
		final List<String> names = Lists.newArrayListWithCapacity(n);
		final List<MemoryByteSource> values = Lists.newArrayListWithCapacity(n);
		for (Entry<String, MemoryByteSource> d : data.entrySet()) {
			names.add(d.getKey());
			values.add(d.getValue());
		}
		List<ByteString> keys = putAll(values);
		if (keys.size() != n) {
			throw new IllegalStateException("Invalid number of keys");
		}
		final ImmutableMap.Builder<String, ByteString> builder = ImmutableMap.builder();
		for (int i = 0; i < n; i++) {
			builder.put(names.get(i), keys.get(i));
		}
		return builder.build();
	}

	/**
	 * Puts a loaded zip file into the repository trying to compress them individually with gzip.
	 * @return A map from the zip entry names to their keys, indicating if the entry has been
	 *         compressed.
	 * @throws BocasException if an error occurs.
	 */
	public Map<String, MaybeCompressed<ByteString>> putZipAndGZip(LoadedZipFile data) throws IOException {
		if (data == null || data.isEmpty()) {
			return ImmutableMap.of();
		}
		final int n = data.size();
		final List<String> names = Lists.newArrayListWithCapacity(n);
		final List<MemoryByteSource> values = Lists.newArrayListWithCapacity(n);
		final List<Boolean> compressed = Lists.newArrayListWithCapacity(n);
		for (Entry<String, MaybeCompressed<MemoryByteSource>> d : data.maybeGzip().entrySet()) {
			names.add(d.getKey());
			compressed.add(d.getValue().isCompressed());
			values.add(d.getValue().getPayload());
		}
		List<ByteString> keys = putAll(values);
		if (keys.size() != n) {
			throw new IllegalStateException("Invalid number of keys");
		}
		final ImmutableMap.Builder<String, MaybeCompressed<ByteString>> builder = ImmutableMap.builder();
		for (int i = 0; i < n; i++) {
			builder.put(names.get(i), MaybeCompressed.of(compressed.get(i), keys.get(i)));
		}
		return builder.build();
	}
}
