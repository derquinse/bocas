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
import net.derquinse.common.base.ByteString;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;

/**
 * Guava-based cache shared for multiple services. Writes are always propagated.
 * @author Andres Rodriguez.
 */
public final class MultiServiceGuavaCache {
	/** Whether the cache is direct. */
	private final boolean direct;
	/** Shared cache. */
	private final Cache<ByteString, LoadedBocasValue> cache;
	/** Service cache. */
	private final LoadingCache<BocasService, Service> serviceCache = CacheBuilder.newBuilder().weakKeys()
			.build(new ServiceLoader());

	/** Constructor. */
	MultiServiceGuavaCache(boolean direct, Cache<ByteString, LoadedBocasValue> cache) {
		this.direct = direct;
		this.cache = checkNotNull(cache);
	}

	/** Decorates a service, adding it to the cache. */
	public CachingBocasService decorate(BocasService service) {
		return serviceCache.getUnchecked(checkNotNull(service, "The service to decorate must be provided"));
	}

	private final class Service implements CachingBocasService {
		/** Service. */
		private final BocasService service;
		/** Bucket cache. */
		private final LoadingCache<String, Bocas> bucketCache = CacheBuilder.newBuilder().build(new BucketLoader());

		Service(BocasService service) {
			this.service = service;
		}

		@Override
		public Bocas getBucket(String name) {
			return bucketCache.getUnchecked(checkNotNull(name, "The bucket name must be provided"));
		}

		@Override
		public CacheStats stats() {
			return cache.stats();
		}

		private final class BucketLoader extends CacheLoader<String, Bocas> {
			@Override
			public Bocas load(String key) throws Exception {
				return new SharedGuavaCachingBocas(service.getBucket(key), cache, direct, true);
			}
		}

	}

	private final class ServiceLoader extends CacheLoader<BocasService, Service> {
		@Override
		public Service load(BocasService key) throws Exception {
			return new Service(key);
		}
	}
}
