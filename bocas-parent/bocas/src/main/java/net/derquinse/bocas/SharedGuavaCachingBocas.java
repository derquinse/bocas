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

import com.google.common.cache.Cache;
import com.google.common.collect.Sets;

/**
 * Guava-cache-based bocas caching bucket that only uses the entry hash as a key.
 * @author Andres Rodriguez.
 */
final class SharedGuavaCachingBocas extends AbstractGuavaCachingBocas<ByteString> {
	/** Constructor. */
	SharedGuavaCachingBocas(Bocas bocas, Cache<ByteString, LoadedBocasValue> cache, boolean direct, boolean alwaysWrite) {
		super(bocas, cache, direct, alwaysWrite);
	}

	@Override
	Callable<LoadedBocasValue> getLoader(final ByteString internalKey) {
		return new Callable<LoadedBocasValue>() {
			@Override
			public LoadedBocasValue call() throws Exception {
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

	<V extends BocasValue> Map<ByteString, V> toInternalEntryMap(Map<ByteString, V> entries) {
		return entries;
	}
}
