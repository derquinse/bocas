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

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Guava-cache-based bocas caching repositories that does not share the cache among every available
 * bucket.
 * @author Andres Rodriguez.
 */
final class BucketGuavaCachingBocasService extends AbstractGuavaCachingBocasService<BucketKey> {
	/** Constructor. */
	BucketGuavaCachingBocasService(BocasService service, CacheBuilder<Object, Object> builder, boolean direct,
			boolean alwaysWrite) {
		super(service, builder, direct, alwaysWrite);
	}

	AbstractBucket createBucket(String name, Bocas source) {
		return new Bucket(source);
	}

	/**
	 * Bucket implementation.
	 * @author Andres Rodriguez.
	 */
	final class Bucket extends AbstractBucket {
		/** Constructor. */
		Bucket(Bocas bocas) {
			super(bocas);
		}

		@Override
		Callable<LoadedBocasValue> getLoader(final BucketKey internalKey) {
			return new Callable<LoadedBocasValue>() {
				@Override
				public LoadedBocasValue call() throws Exception {
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

		<V extends BocasValue> Map<BucketKey, V> toInternalEntryMap(Map<ByteString, V> entries) {
			final Map<BucketKey, V> map = Maps.newHashMapWithExpectedSize(entries.size());
			for (Entry<ByteString, V> entry : entries.entrySet()) {
				map.put(new BucketKey(this, entry.getKey()), entry.getValue());
			}
			return map;
		}
	}
}
