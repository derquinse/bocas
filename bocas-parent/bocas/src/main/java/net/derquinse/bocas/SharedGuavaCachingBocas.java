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
import java.util.concurrent.Callable;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.cache.Cache;
import com.google.common.collect.Sets;

/**
 * Guava-cache-based bocas caching bucket that only uses the entry hash as a key.
 * @author Andres Rodriguez.
 */
final class SharedGuavaCachingBocas extends AbstractGuavaCachingBocas<ByteString> {
	/** Constructor. */
	SharedGuavaCachingBocas(Bocas bocas, MemoryByteSourceLoader loader, Cache<ByteString, MemoryByteSource> cache,
			boolean alwaysWrite) {
		super(bocas, loader, cache, alwaysWrite);
	}

	@Override
	Callable<MemoryByteSource> getLoader(final ByteString internalKey) {
		return new Callable<MemoryByteSource>() {
			@Override
			public MemoryByteSource call() throws Exception {
				return loadFromSource(internalKey);
			}
		};
	}

	ByteString toInternalKey(ByteString key) {
		return key;
	}

	ByteString toKey(ByteString internalKey) {
		return internalKey;
	}

	Set<ByteString> toInternalKeySet(Iterable<ByteString> keys) {
		final Set<ByteString> set;
		if (keys instanceof Set) {
			set = (Set<ByteString>) keys;
		} else {
			set = Sets.newHashSet(keys);
		}
		return set;
	}

	Set<ByteString> toKeySet(Set<ByteString> internalKeys) {
		return internalKeys;
	}

	Map<ByteString, MemoryByteSource> toInternalEntryMap(Map<ByteString, MemoryByteSource> entries) {
		return entries;
	}
}
