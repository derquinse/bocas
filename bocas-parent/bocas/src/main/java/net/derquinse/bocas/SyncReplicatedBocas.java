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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.newHashMap;

import java.util.Map;
import java.util.Set;

import net.derquinse.common.base.ByteString;

import com.google.common.base.Optional;

/**
 * Bocas transformer that synchronously replicates operation in another bucket.
 * @author Andres Rodriguez.
 */
final class SyncReplicatedBocas extends SkeletalBocas {
	/** Primary bucket. */
	private final Bocas primary;
	/** Replica bucket. */
	private final Bocas replica;
	/** Whether to check for existing entries before writing. */
	private final boolean checkBeforeWrite;

	/** Constructor. */
	SyncReplicatedBocas(Bocas primary, Bocas replica, boolean checkBeforeWrite) {
		this.primary = checkNotNull(primary, "The primary bucket must be provided.");
		this.replica = checkNotNull(replica, "The replica bucket must be provided.");
		checkArgument(primary != replica, "The primary and replica bucket can't be the same");
		this.checkBeforeWrite = checkBeforeWrite;
	}

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
	public boolean contains(ByteString key) {
		return primary.contains(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public Set<ByteString> contained(Iterable<ByteString> keys) {
		return primary.contained(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<BocasValue> get(ByteString key) {
		return primary.get(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, BocasValue> get(Iterable<ByteString> keys) {
		return primary.get(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#load(net.derquinse.bocas.BocasValue)
	 */
	@Override
	protected LoadedBocasValue load(BocasValue value) {
		return value.toLoaded();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#put(net.derquinse.common.base.ByteString,
	 * net.derquinse.bocas.LoadedBocasValue)
	 */
	@Override
	protected void put(ByteString key, LoadedBocasValue value) {
		if (checkBeforeWrite && contains(key)) {
			return; // nothing to do
		}
		replica.put(value);
		primary.put(value);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.SkeletalBocas#putAll(java.util.Map)
	 */
	@Override
	protected void putAll(Map<ByteString, LoadedBocasValue> entries) {
		final Map<ByteString, LoadedBocasValue> map;
		if (checkBeforeWrite) {
			Set<ByteString> found = contained(entries.keySet());
			if (found.isEmpty()) {
				map = entries;
			} else {
				map = newHashMap(filterKeys(entries, not(in(found))));
			}
		} else {
			map = entries;
		}
		replica.putAll(map.values());
		primary.putAll(map.values());
	}

}