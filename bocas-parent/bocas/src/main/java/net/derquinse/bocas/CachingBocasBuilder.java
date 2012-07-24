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

import java.util.concurrent.TimeUnit;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

/**
 * Builder for caching Bocas repositories.
 * @author Andres Rodriguez.
 */
@Beta
public final class CachingBocasBuilder {
	/** Internal builder. */
	private final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().recordStats();

	/** Constructor. */
	CachingBocasBuilder() {
	}

	/**
	 * Specifies the maximum number of entries the cache may contain. Note that the cache <b>may evict
	 * an entry before this limit is exceeded</b>.
	 * <p>
	 * When {@code size} is zero, elements will be evicted immediately after being loaded into the
	 * cache. This can be useful in testing, or to disable caching temporarily without a code change.
	 * @param size the maximum size of the cache
	 * @throws IllegalArgumentException if {@code size} is negative
	 * @throws IllegalStateException if a maximum size was already set
	 */
	public CachingBocasBuilder maximumSize(long size) {
		builder.maximumSize(size);
		return this;
	}

	/**
	 * Specifies the maximum weight of entries the cache may contain. The weight of an entry is the
	 * number of bytes of its value.
	 * <p>
	 * Note that the cache <b>may evict an entry before this limit is exceeded</b>.
	 * <p>
	 * When {@code weight} is zero, elements will be evicted immediately after being loaded into
	 * cache. This can be useful in testing, or to disable caching temporarily without a code change.
	 * <p>
	 * Note that weight is only used to determine whether the cache is over capacity; it has no effect
	 * on selecting which entry should be evicted next.
	 * @param weight the maximum weight the cache may contain
	 * @throws IllegalArgumentException if {@code size} is negative
	 * @throws IllegalStateException if a maximum size was already set
	 */
	public CachingBocasBuilder maximumWeight(long weight) {
		builder.maximumWeight(weight).weigher(EntryWeigher.INSTANCE);
		return this;
	}

	/**
	 * Specifies that each entry should be automatically removed from the cache once a fixed duration
	 * has elapsed after the entry's creation, the most recent replacement of its value, or its last
	 * access.
	 * <p>
	 * When {@code duration} is zero, this method hands off to {@link #maximumSize(long) maximumSize}
	 * {@code (0)}, ignoring any otherwise-specificed maximum size or weight. This can be useful in
	 * testing, or to disable caching temporarily without a code change.
	 * @param duration the length of time after an entry is last accessed that it should be
	 *          automatically removed
	 * @param unit the unit that {@code duration} is expressed in
	 * @throws IllegalArgumentException if {@code duration} is negative
	 * @throws IllegalStateException if the time to idle or time to live was already set
	 */
	public CachingBocasBuilder expireAfterAccess(long duration, TimeUnit unit) {
		builder.expireAfterAccess(duration, unit);
		return this;
	}

	/**
	 * Builds a cache. Soft values are used.
	 * @param bocas Repository to cache.
	 * @return The caching repository.
	 */
	public CachingBocas build(Bocas bocas) {
		return new GuavaCachingBocas(builder.softValues(), bocas);
	}

	/**
	 * Builds a cache based on direct buffers.
	 * @param bocas Repository to cache.
	 * @return The caching repository.
	 */
	public CachingBocas buildDirect(Bocas bocas) {
		return new DirectGuavaCachingBocas(builder, bocas);
	}

	private enum EntryWeigher implements Weigher<ByteString, BocasValue> {
		INSTANCE;

		@Override
		public int weigh(ByteString key, BocasValue value) {
			return value.getSize();
		}
	}

}
