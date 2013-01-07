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
 * Abstract class for memory-based bocas bucket.
 * @author Andres Rodriguez.
 */
@Beta
abstract class AbstractMemoryBocas extends SkeletalBocas {
	/** Repository. */
	private final ConcurrentMap<ByteString, LoadedBocasValue> bucket = new MapMaker().makeMap();

	/** Constructor. */
	AbstractMemoryBocas() {
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public final boolean contains(ByteString key) {
		return bucket.containsKey(key);
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
		return Sets.intersection(requested, bucket.keySet()).immutableCopy();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public final Optional<BocasValue> get(ByteString key) {
		BocasValue value = bucket.get(key);
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
			BocasValue value = bucket.get(key);
			if (value != null) {
				map.put(key, value);
			}
		}
		return map;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#put(net.derquinse.common.base.ByteString,
	 * net.derquinse.bocas.LoadedBocasValue)
	 */
	@Override
	protected void put(ByteString key, LoadedBocasValue value) {
		bucket.putIfAbsent(key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#putAll(java.util.Map)
	 */
	@Override
	protected void putAll(Map<ByteString, LoadedBocasValue> entries) {
		bucket.putAll(entries);
	}

}
