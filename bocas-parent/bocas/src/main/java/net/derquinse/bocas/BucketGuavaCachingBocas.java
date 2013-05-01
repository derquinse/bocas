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
import java.util.concurrent.Callable;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.cache.Cache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Guava-cache-based bocas caching bucket that uses the bucket itself and the entry hash as a key.
 * @author Andres Rodriguez.
 */
final class BucketGuavaCachingBocas extends AbstractGuavaCachingBocas<BucketKey> {
	/** Constructor. */
	BucketGuavaCachingBocas(Bocas bocas, MemoryByteSourceLoader loader, Cache<BucketKey, MemoryByteSource> cache,
			boolean alwaysWrite) {
		super(bocas, loader, cache, alwaysWrite);
	}

	@Override
	Callable<MemoryByteSource> getLoader(final BucketKey internalKey) {
		return new Callable<MemoryByteSource>() {
			@Override
			public MemoryByteSource call() throws Exception {
				return loadFromSource(internalKey.getKey());
			}
		};
	}

	BucketKey toInternalKey(ByteString key) {
		return new BucketKey(this, key);
	}

	ByteString toKey(BucketKey internalKey) {
		return internalKey.getKey();
	}

	Set<BucketKey> toInternalKeySet(Iterable<ByteString> keys) {
		final Set<BucketKey> set = Sets.newHashSet();
		for (ByteString key : keys) {
			set.add(new BucketKey(this, key));
		}
		return set;
	}

	Set<ByteString> toKeySet(Set<BucketKey> internalKeys) {
		final Set<ByteString> set = Sets.newHashSetWithExpectedSize(internalKeys.size());
		for (BucketKey key : internalKeys) {
			set.add(key.getKey());
		}
		return set;
	}

	Map<BucketKey, MemoryByteSource> toInternalEntryMap(Map<ByteString, MemoryByteSource> entries) {
		final Map<BucketKey, MemoryByteSource> map = Maps.newHashMapWithExpectedSize(entries.size());
		for (Entry<ByteString, MemoryByteSource> entry : entries.entrySet()) {
			map.put(new BucketKey(this, entry.getKey()), entry.getValue());
		}
		return map;
	}
}
