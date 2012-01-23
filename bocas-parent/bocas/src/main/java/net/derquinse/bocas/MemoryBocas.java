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
import java.util.concurrent.ConcurrentMap;

import net.derquinse.common.base.ByteString;

import com.beust.jcommander.internal.Maps;
import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;

/**
 * A memory-based BOCAS repository.
 * @author Andres Rodriguez.
 */
@Beta
final class MemoryBocas implements Bocas {
	/** Repository. */
	private final ConcurrentMap<ByteString, InputSupplier<InputStream>> repository = new MapMaker().makeMap();

	/** Constructor. */
	MemoryBocas() {
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public boolean contains(ByteString key) {
		return repository.containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public Set<ByteString> contained(Iterable<ByteString> keys) {
		final Set<ByteString> requested;
		if (keys instanceof Set) {
			requested = (Set<ByteString>) keys;
		} else {
			requested = Sets.newHashSet(keys);
		}
		if (requested.isEmpty()) {
			return ImmutableSet.of();
		}
		return Sets.intersection(requested, repository.keySet()).immutableCopy();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<InputSupplier<InputStream>> get(ByteString key) {
		InputSupplier<InputStream> data = repository.get(key);
		return Optional.fromNullable(data);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, InputSupplier<InputStream>> get(Iterable<ByteString> keys) {
		Map<ByteString, InputSupplier<InputStream>> map = Maps.newHashMap();
		for (ByteString key : keys) {
			InputSupplier<InputStream> data = repository.get(key);
			if (data != null) {
				map.put(key, data);
			}
		}
		return map;
	}

	private ByteString put(BocasEntry entry) {
		ByteString key = entry.getKey();
		repository.putIfAbsent(key, entry.getValue());
		return key;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(com.google.common.io.InputSupplier)
	 */
	@Override
	public ByteString put(InputSupplier<? extends InputStream> object) {
		return put(BocasEntry.loaded(object));
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putAll(java.util.List)
	 */
	@Override
	public List<ByteString> putAll(List<? extends InputSupplier<? extends InputStream>> objects) {
		List<BocasEntry> entries = Lists.newLinkedList();
		for (InputSupplier<? extends InputStream> object : objects) {
			entries.add(BocasEntry.loaded(object));
		}
		List<ByteString> keys = Lists.newArrayListWithCapacity(entries.size());
		for (BocasEntry entry : entries) {
			keys.add(put(entry));
		}
		return keys;
	}

}
