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

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * Abstract base class for Guava-cache-based bocas buckets.
 * @author Andres Rodriguez.
 */
abstract class AbstractGuavaCachingBocas<K> extends SkeletalBocas {
	/** Cached bucket. */
	private final Bocas bocas;
	/** Entry Cache. */
	private final Cache<K, LoadedBocasValue> cache;
	/** Whether the cache is direct. */
	private final boolean direct;
	/** Whether writes are always performed. */
	private final boolean alwaysWrite;

	/** Constructor. */
	AbstractGuavaCachingBocas(Bocas bocas, Cache<K, LoadedBocasValue> cache, boolean direct, boolean alwaysWrite) {
		this.bocas = checkNotNull(bocas, "The bucket to cache must be provided");
		this.cache = checkNotNull(cache, "The cache to use must be provided");
		this.direct = direct;
		this.alwaysWrite = alwaysWrite;
	}

	@Override
	protected final LoadedBocasValue load(BocasValue value) {
		return direct ? value.toDirect() : value.toHeap();
	}

	final LoadedBocasValue loadFromSource(ByteString key) {
		Optional<BocasValue> v = bocas.get(key);
		if (v.isPresent()) {
			return load(v.get());
		}
		throw new EntryNotFoundException();
	}

	abstract Callable<LoadedBocasValue> getLoader(K internalKey);

	abstract K toInternalKey(ByteString key);

	abstract ByteString toKey(K internalKey);

	abstract Set<K> toInternalKeySet(Iterable<ByteString> keys);

	abstract Set<ByteString> toKeySet(Set<K> internalKeys);

	abstract <V extends BocasValue> Map<K, V> toInternalEntryMap(Map<ByteString, V> entries);

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
	public final Optional<BocasValue> get(ByteString key) {
		try {
			K internalKey = toInternalKey(key);
			BocasValue v = cache.get(internalKey, getLoader(internalKey));
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
	public final Map<ByteString, BocasValue> get(Iterable<ByteString> keys) {
		Set<K> ikRequested = toInternalKeySet(keys);
		if (ikRequested.isEmpty()) {
			return ImmutableMap.of();
		}
		Map<ByteString, BocasValue> found = Maps.newHashMapWithExpectedSize(ikRequested.size());
		Set<ByteString> notCached = Sets.newHashSetWithExpectedSize(ikRequested.size());
		for (K internalKey : ikRequested) {
			ByteString key = toKey(internalKey);
			BocasValue value = cache.getIfPresent(internalKey);
			if (value != null) {
				found.put(key, value);
			} else {
				notCached.add(key);
			}
		}
		Map<ByteString, BocasValue> foundNotCached = bocas.get(notCached);
		for (Entry<ByteString, BocasValue> e : foundNotCached.entrySet()) {
			ByteString key = e.getKey();
			K internalKey = toInternalKey(key);
			LoadedBocasValue value = load(e.getValue());
			cache.put(internalKey, value);
			found.put(key, value);
		}
		return found;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#put(net.derquinse.common.base.ByteString,
	 * net.derquinse.bocas.LoadedBocasValue)
	 */
	@Override
	protected void put(ByteString key, LoadedBocasValue value) {
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
	protected void putAll(Map<ByteString, LoadedBocasValue> entries) {
		if (alwaysWrite) {
			bocas.putAll(entries.values());
		}
		final Map<K, LoadedBocasValue> map = cache.asMap();
		final Map<K, LoadedBocasValue> notCached = Maps.filterKeys(toInternalEntryMap(entries),
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