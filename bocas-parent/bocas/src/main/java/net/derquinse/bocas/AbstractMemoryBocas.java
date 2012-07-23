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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Abstract class for memory-based Bocas repositories.
 * @author Andres Rodriguez.
 */
@Beta
abstract class AbstractMemoryBocas<V extends BocasValue> extends SkeletalBocasBackend {
	/** Repository. */
	private final ConcurrentMap<ByteString, V> repository = new MapMaker().makeMap();

	/** Constructor. */
	AbstractMemoryBocas() {
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public final boolean contains(ByteString key) {
		return repository.containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public final Set<ByteString> contained(Iterable<ByteString> keys) {
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
	public final Optional<BocasValue> get(ByteString key) {
		BocasValue value = repository.get(key);
		return Optional.fromNullable(value);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public final Map<ByteString, BocasValue> get(Iterable<ByteString> keys) {
		Map<ByteString, BocasValue> map = Maps.newHashMap();
		for (ByteString key : keys) {
			BocasValue value = repository.get(key);
			if (value != null) {
				map.put(key, value);
			}
		}
		return map;
	}

	/** Turns a loaded bocas value into a value of the correct type. */
	abstract V toValue(LoadedBocasValue value);

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocasBackend#put(net.derquinse.bocas.LoadedBocasEntry)
	 */
	@Override
	protected final void put(LoadedBocasEntry entry) {
		repository.putIfAbsent(entry.getKey(), toValue(entry.getValue()));
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocasBackend#put(java.util.Map)
	 */
	@Override
	protected final void put(Map<ByteString, LoadedBocasValue> entries) {
		for (Entry<ByteString, LoadedBocasValue> entry : entries.entrySet()) {
			repository.putIfAbsent(entry.getKey(), toValue(entry.getValue()));
		}
	}
}
