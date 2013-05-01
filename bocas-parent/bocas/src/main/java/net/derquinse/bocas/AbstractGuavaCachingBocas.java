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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Abstract base class for Guava-cache-based bocas buckets.
 * @author Andres Rodriguez.
 */
abstract class AbstractGuavaCachingBocas<K> extends AbstractMemoryBocas {
	/** Cached bucket. */
	private final Bocas bocas;
	/** Entry Cache. */
	private final Cache<K, MemoryByteSource> cache;
	/** Whether writes are always performed. */
	private final boolean alwaysWrite;

	/** Constructor. */
	AbstractGuavaCachingBocas(Bocas bocas, MemoryByteSourceLoader loader, Cache<K, MemoryByteSource> cache,
			boolean alwaysWrite) {
		super(checkNotNull(bocas, "The bucket to cache must be provided").getHashFunction(), loader);
		this.bocas = bocas;
		this.cache = checkNotNull(cache, "The cache to use must be provided");
		this.alwaysWrite = alwaysWrite;
	}

	final MemoryByteSource loadFromSource(ByteString key) {
		Optional<ByteSource> v = bocas.get(key);
		if (v.isPresent()) {
			return transform(v.get());
		}
		throw new EntryNotFoundException();
	}

	abstract Callable<MemoryByteSource> getLoader(K internalKey);

	abstract K toInternalKey(ByteString key);

	abstract ByteString toKey(K internalKey);

	abstract Set<K> toInternalKeySet(Iterable<ByteString> keys);

	abstract Set<ByteString> toKeySet(Set<K> internalKeys);

	abstract Map<K, MemoryByteSource> toInternalEntryMap(Map<ByteString, MemoryByteSource> entries);

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#close()
	 */
	@Override
	public void close() {
		// Nothing to do.
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public final boolean contains(ByteString key) {
		final K internalKey = toInternalKey(key);
		if (cache.asMap().containsKey(internalKey)) {
			return true;
		}
		return bocas.contains(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public final Set<ByteString> contained(Iterable<ByteString> keys) {
		Set<K> ikRequested = toInternalKeySet(keys);
		if (ikRequested.isEmpty()) {
			return ImmutableSet.of();
		}
		Set<K> ikCached = Sets.intersection(ikRequested, cache.asMap().keySet()).immutableCopy();
		Set<K> ikNotCached = Sets.difference(ikRequested, ikCached).immutableCopy();
		Set<ByteString> kCached = toKeySet(ikCached);
		if (ikNotCached.isEmpty()) {
			return kCached;
		}
		Set<ByteString> kFound = bocas.contained(toKeySet(ikNotCached));
		return Sets.union(kCached, kFound).immutableCopy();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public final Optional<ByteSource> get(ByteString key) {
		try {
			K internalKey = toInternalKey(key);
			ByteSource v = cache.get(internalKey, getLoader(internalKey));
			return Optional.of(v);
		} catch (UncheckedExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof EntryNotFoundException) {
				return Optional.absent();
			}
			if (cause instanceof BocasException) {
				throw (BocasException) cause;
			}
			throw new BocasException(cause);
		} catch (ExecutionException e) {
			throw new BocasException(e.getCause());
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public final Map<ByteString, ByteSource> get(Iterable<ByteString> keys) {
		Set<K> ikRequested = toInternalKeySet(keys);
		if (ikRequested.isEmpty()) {
			return ImmutableMap.of();
		}
		Map<ByteString, ByteSource> found = Maps.newHashMapWithExpectedSize(ikRequested.size());
		Set<ByteString> notCached = Sets.newHashSetWithExpectedSize(ikRequested.size());
		for (K internalKey : ikRequested) {
			ByteString key = toKey(internalKey);
			ByteSource value = cache.getIfPresent(internalKey);
			if (value != null) {
				found.put(key, value);
			} else {
				notCached.add(key);
			}
		}
		Map<ByteString, ByteSource> foundNotCached = bocas.get(notCached);
		for (Entry<ByteString, ByteSource> e : foundNotCached.entrySet()) {
			ByteString key = e.getKey();
			K internalKey = toInternalKey(key);
			MemoryByteSource value = transform(e.getValue());
			cache.put(internalKey, value);
			found.put(key, value);
		}
		return found;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#put(net.derquinse.common.base.ByteString,
	 * com.google.common.io.ByteSource)
	 */
	@Override
	protected void put(ByteString key, MemoryByteSource value) {
		if (alwaysWrite) {
			bocas.put(value);
		}
		K internalKey = toInternalKey(key);
		if (!cache.asMap().containsKey(internalKey)) {
			if (!alwaysWrite) {
				// otherwise, already written
				bocas.put(value);
			}
			cache.put(internalKey, value);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#putAll(java.util.Map)
	 */
	@Override
	protected void putAll(Map<ByteString, MemoryByteSource> entries) {
		if (alwaysWrite) {
			bocas.putAll(entries.values());
		}
		final Map<K, MemoryByteSource> map = cache.asMap();
		final Map<K, MemoryByteSource> notCached = Maps.filterKeys(toInternalEntryMap(entries),
				Predicates.not(Predicates.in(map.keySet())));
		if (notCached.isEmpty()) {
			return;
		}
		if (!alwaysWrite) {
			bocas.putAll(notCached.values());
		}
		map.putAll(notCached);
	}
}
