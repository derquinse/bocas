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

import static com.google.common.base.Preconditions.checkState;
import static net.derquinse.bocas.InternalUtils.checkLoader;

import java.util.concurrent.TimeUnit;

import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.cache.CacheBuilder;

/**
 * Builder for caching bocas repositories based on Guava.
 * @author Andres Rodriguez.
 */
public final class GuavaCachingBocasBuilder {
	/** Whether the service has already been built. */
	private boolean built = false;
	/** Memory loader to use. */
	private MemoryByteSourceLoader loader = MemoryByteSourceLoader.get();
	/** Whether the cache is shared among the available buckets. */
	private boolean shared = false;
	/** Whether writes are always performed. */
	private Boolean alwaysWrite = null;
	/** Internal builder. */
	private final CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().recordStats();

	/** Constructor. */
	GuavaCachingBocasBuilder() {
	}

	private void checkNotBuilt() {
		checkState(!built, "The service has already been built");
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
	 * @throws IllegalStateException if the service has already been built
	 */
	public GuavaCachingBocasBuilder maximumSize(long size) {
		checkNotBuilt();
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
	 * @throws IllegalStateException if the service has already been built
	 */
	public GuavaCachingBocasBuilder maximumWeight(long weight) {
		checkNotBuilt();
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
	 * @throws IllegalStateException if the service has already been built
	 */
	public GuavaCachingBocasBuilder expireAfterAccess(long duration, TimeUnit unit) {
		checkNotBuilt();
		builder.expireAfterAccess(duration, unit);
		return this;
	}

	/**
	 * Specifies the memory loader tu use.
	 * @throws IllegalStateException if the service has already been built
	 */
	public GuavaCachingBocasBuilder loader(MemoryByteSourceLoader loader) {
		checkNotBuilt();
		this.loader = checkLoader(loader);
		return this;
	}

	/**
	 * Specifies if the cached values will be shared among the available buckets.
	 * @throws IllegalStateException if the service has already been built
	 */
	public GuavaCachingBocasBuilder shared() {
		checkNotBuilt();
		this.shared = true;
		return this;
	}

	/**
	 * Specifies whether writes are always propagated to the source service. For shared caches the
	 * default value is true, otherwise is false.
	 * @throws IllegalStateException if the value has already been set
	 * @throws IllegalStateException if the service has already been built
	 */
	public GuavaCachingBocasBuilder alwaysWrite(boolean alwaysWrite) {
		checkNotBuilt();
		checkState(this.alwaysWrite == null, "The alwaysWrite flag has already been set");
		this.alwaysWrite = alwaysWrite;
		return this;
	}

	/**
	 * Builds a cache.
	 * @param service Repository to cache.
	 * @return The caching repository.
	 */
	public CachingBocasService build(BocasService service) {
		checkNotBuilt();
		built = true;
		final boolean write = alwaysWrite != null ? alwaysWrite.booleanValue() : shared;
		if (shared) {
			return new SharedGuavaCachingBocasService(service, loader, builder, write);
		} else {
			return new BucketGuavaCachingBocasService(service, loader, builder, write);
		}
	}

}
