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

import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * A Guava-cache-based Bocas caching repository.
 * @author Andres Rodriguez.
 */
@Beta
final class GuavaCachingBocas extends SkeletalBocasBackend implements CachingBocas {
	/** Cached repository. */
	private final Bocas bocas;
	/** Cache. */
	private final LoadingCache<ByteString, LoadedBocasValue> cache;

	/** Constructor. */
	GuavaCachingBocas(Bocas bocas, Long weight, Long maxSize, long duration, TimeUnit unit) {
		this.bocas = Preconditions.checkNotNull(bocas);
		CacheBuilder<Object, Object> builder = CacheBuilder.newBuilder().expireAfterAccess(duration, unit);
		if (maxSize != null) {
			builder.maximumSize(maxSize);
		}
		if (weight != null) {
			builder.maximumWeight(weight).weigher(EntryWeigher.INSTANCE);
		}
		this.cache = builder.recordStats().build(new Loader());
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public boolean contains(ByteString key) {
		if (cache.asMap().containsKey(key)) {
			return true;
		}
		return bocas.contains(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public Set<ByteString> contained(Iterable<ByteString> keys) {
		final Set<ByteString> requested;
		if (keys instanceof Set) {
			requested = (Set<ByteString>) keys;
		} else {
			requested = Sets.newHashSet(keys);
		}
		if (requested.isEmpty()) {
			return ImmutableSet.of();
		}
		Set<ByteString> cached = Sets.intersection(requested, cache.asMap().keySet()).immutableCopy();
		Set<ByteString> notCached = Sets.difference(requested, cached).immutableCopy();
		if (notCached.isEmpty()) {
			return cached;
		}
		Set<ByteString> found = bocas.contained(notCached);
		return Sets.union(cached, found).immutableCopy();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<BocasValue> get(ByteString key) {
		try {
			BocasValue v = cache.getUnchecked(key);
			return Optional.of(v);
		} catch (UncheckedExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof NotFoundException) {
				return Optional.absent();
			}
			if (cause instanceof BocasException) {
				throw (BocasException) cause;
			}
			throw new BocasException(cause);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, BocasValue> get(Iterable<ByteString> keys) {
		final Set<ByteString> requested;
		if (keys instanceof Set) {
			requested = (Set<ByteString>) keys;
		} else {
			requested = Sets.newHashSet(keys);
		}
		if (requested.isEmpty()) {
			return ImmutableMap.of();
		}
		Map<ByteString, BocasValue> found = Maps.newHashMapWithExpectedSize(requested.size());
		Set<ByteString> notCached = Sets.newHashSetWithExpectedSize(requested.size());
		for (ByteString key : requested) {
			LoadedBocasValue value = cache.getIfPresent(key);
			if (value != null) {
				found.put(key, value);
			} else {
				notCached.add(key);
			}
		}
		Map<ByteString, BocasValue> foundInCached = bocas.get(notCached);
		for (Entry<ByteString, BocasValue> e : foundInCached.entrySet()) {
			ByteString k = e.getKey();
			LoadedBocasValue v = e.getValue().load();
			cache.put(k, v);
			found.put(k, v);
		}
		return found;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocasBackend#put(net.derquinse.bocas.LoadedBocasEntry)
	 */
	@Override
	protected void put(LoadedBocasEntry entry) {
		ByteString key = entry.getKey();
		LoadedBocasValue value = entry.getValue();
		if (!cache.asMap().containsKey(key)) {
			bocas.put(value);
			cache.put(key, value);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocasBackend#put(java.util.Map)
	 */
	@Override
	protected void put(Map<ByteString, LoadedBocasValue> entries) {
		final Map<ByteString, LoadedBocasValue> notCached = Maps.newHashMap();
		final Map<ByteString, LoadedBocasValue> map = cache.asMap();
		for (Entry<ByteString, LoadedBocasValue> entry : entries.entrySet()) {
			if (!map.containsKey(entry.getKey())) {
				notCached.put(entry.getKey(), entry.getValue());
			}
		}
		if (notCached.isEmpty()) {
			return;
		}
		bocas.putSuppliers(Lists.newLinkedList(notCached.values()));
		map.putAll(notCached);
	}

	@SuppressWarnings("serial")
	private static final class NotFoundException extends RuntimeException {
		NotFoundException() {
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocasBackend#putZip(java.io.InputStream)
	 */
	@Override
	public Map<String, ByteString> putZip(InputStream object) {
		return bocas.putZip(object);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocasBackend#putZip(com.google.common.io.InputSupplier)
	 */
	@Override
	public Map<String, ByteString> putZip(InputSupplier<? extends InputStream> object) {
		return bocas.putZip(object);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.CachingBocas#stats()
	 */
	@Override
	public CacheStats stats() {
		return cache.stats();
	}

	private final class Loader extends CacheLoader<ByteString, LoadedBocasValue> {
		@Override
		public LoadedBocasValue load(ByteString key) throws Exception {
			Optional<BocasValue> v = bocas.get(key);
			if (v.isPresent()) {
				return v.get().load();
			}
			throw new NotFoundException();
		}
	}

	private enum EntryWeigher implements Weigher<ByteString, LoadedBocasValue> {
		INSTANCE;

		@Override
		public int weigh(ByteString key, LoadedBocasValue value) {
			return value.getSize();
		}
	}

}
