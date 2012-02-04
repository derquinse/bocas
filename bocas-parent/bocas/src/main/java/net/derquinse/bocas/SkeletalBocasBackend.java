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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.util.zip.ZipFiles;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;

/**
 * A base implementation for a Bocas repository back-end. This abstract class should not be used for
 * caches, proxies, stubs or any other class that is a final back-end.
 * @author Andres Rodriguez.
 */
@Beta
public abstract class SkeletalBocasBackend implements Bocas {
	/** Constructor. */
	protected SkeletalBocasBackend() {
	}

	/**
	 * Puts an entry into the repository.
	 * @throws BocasException if an error occurs.
	 */
	protected abstract void put(LoadedBocasEntry entry);

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(com.google.common.io.InputSupplier)
	 */
	@Override
	public ByteString put(InputSupplier<? extends InputStream> object) {
		LoadedBocasEntry entry = BocasEntry.loaded(object);
		put(entry);
		return entry.getKey();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(java.io.InputStream)
	 */
	@Override
	public ByteString put(InputStream object) {
		LoadedBocasEntry entry = BocasEntry.loaded(object);
		put(entry);
		return entry.getKey();
	}

	/**
	 * Puts some entries into the repository in a single operation.
	 * @throws BocasException if an error occurs.
	 */
	protected abstract void put(Iterable<? extends LoadedBocasEntry> entries);

	/** Returns a list of keys. */
	private List<ByteString> getKeys(Iterable<? extends BocasEntry> entries) {
		return ImmutableList.copyOf(Iterables.transform(entries, BocasEntry.KEY));
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putSuppliers(java.util.List)
	 */
	@Override
	public List<ByteString> putSuppliers(List<? extends InputSupplier<? extends InputStream>> objects) {
		List<LoadedBocasEntry> entries = Lists.newLinkedList();
		for (InputSupplier<? extends InputStream> object : objects) {
			entries.add(BocasEntry.loaded(object));
		}
		put(entries);
		return getKeys(entries);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putStreams(java.util.List)
	 */
	@Override
	public List<ByteString> putStreams(List<? extends InputStream> objects) {
		List<LoadedBocasEntry> entries = Lists.newLinkedList();
		for (InputStream object : objects) {
			entries.add(BocasEntry.loaded(object));
		}
		put(entries);
		return getKeys(entries);
	}

	private Map<String, ByteString> putZip(Map<String, byte[]> data) {
		if (data == null || data.isEmpty()) {
			return ImmutableMap.of();
		}
		Map<String, LoadedBocasEntry> entries = Maps.newHashMapWithExpectedSize(data.size());
		for (Entry<String, byte[]> d : data.entrySet()) {
			entries.put(d.getKey(), BocasEntry.of(d.getValue()));
		}
		put(entries.values());
		return ImmutableMap.copyOf(Maps.transformValues(entries, BocasEntry.KEY));
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putZip(java.io.InputStream)
	 */
	@Override
	public Map<String, ByteString> putZip(InputStream object) {
		try {
			return putZip(ZipFiles.loadZip(object));
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putZip(com.google.common.io.InputSupplier)
	 */
	@Override
	public Map<String, ByteString> putZip(InputSupplier<? extends InputStream> object) {
		try {
			return putZip(ZipFiles.loadZip(object));
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

}
