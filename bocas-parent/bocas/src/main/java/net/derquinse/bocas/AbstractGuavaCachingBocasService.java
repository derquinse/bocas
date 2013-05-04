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

import static com.google.common.base.Preconditions.checkNotNull;
import static net.derquinse.bocas.BocasPreconditions.checkLoader;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Abstract base class for Guava-cache-based bocas caching repositories.
 * @author Andres Rodriguez.
 */
abstract class AbstractGuavaCachingBocasService<K> implements CachingBocasService {
	/** Cached service. */
	private final BocasService service;
	/** Bucket Cache. */
	private final LoadingCache<String, Bocas> bucketCache;
	/** Entry Cache. */
	private final Cache<K, MemoryByteSource> cache;
	/** Memory loader to use. */
	private final MemoryByteSourceLoader loader;
	/** Whether writes are always performed. */
	private final boolean alwaysWrite;

	/** Constructor. */
	AbstractGuavaCachingBocasService(BocasService service, MemoryByteSourceLoader loader,
			CacheBuilder<Object, Object> builder, boolean alwaysWrite) {
		this.service = checkNotNull(service);
		this.loader = checkLoader(loader);
		this.alwaysWrite = alwaysWrite;
		this.bucketCache = CacheBuilder.newBuilder().build(new BucketLoader());
		this.cache = builder.recordStats().build();
	}

	/** Returns the cache to use. */
	final Cache<K, MemoryByteSource> getCache() {
		return cache;
	}

	/** Returns whether writes are always performed. */
	final boolean isAlwaysWrite() {
		return alwaysWrite;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasService#getBucket(java.lang.String)
	 */
	@Override
	public final Bocas getBucket(String name) {
		try {
			return bucketCache.getUnchecked(name);
		} catch (UncheckedExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof EntryNotFoundException) {
				throw new IllegalArgumentException();
			}
			if (cause instanceof BocasException) {
				throw (BocasException) cause;
			}
			throw new BocasException(cause);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.CachingBocasService#stats()
	 */
	@Override
	public CacheStats stats() {
		return cache.stats();
	}

	abstract AbstractGuavaCachingBocas<K> createBucket(Bocas source, MemoryByteSourceLoader loader);

	private final class BucketLoader extends CacheLoader<String, Bocas> {
		@Override
		public Bocas load(String name) throws Exception {
			try {
				return createBucket(service.getBucket(name), loader);
			} catch (IllegalArgumentException e) {
				throw new BucketNotFoundException();
			}
		}
	}

}
