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

import java.util.Map;
import java.util.Set;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A Bocas repository based on a primary and a fallback one. Writes are not propagated to the
 * fallback.
 * @author Andres Rodriguez.
 */
@Beta
final class FallbackBocas extends ForwardingBocas {
	/** Primary repository. */
	private final Bocas primary;
	/** Fallback repository. */
	private final Bocas fallback;

	/** Constructor. */
	FallbackBocas(Bocas primary, Bocas fallback) {
		this.primary = checkNotNull(primary);
		this.fallback = checkNotNull(fallback);
		checkArgument(primary != fallback, "Primary and fallback repositories should not be the same");
	}

	@Override
	protected Bocas delegate() {
		return primary;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public boolean contains(ByteString key) {
		if (primary.contains(key)) {
			return true;
		}
		return fallback.contains(key);
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
		Set<ByteString> inPrimary = primary.contained(requested);
		Set<ByteString> askFallback = Sets.difference(requested, inPrimary).immutableCopy();
		if (askFallback.isEmpty()) {
			return inPrimary;
		}
		Set<ByteString> inSecondary = fallback.contained(askFallback);
		return Sets.union(inPrimary, inSecondary).immutableCopy();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<BocasValue> get(ByteString key) {
		Optional<BocasValue> p = primary.get(key);
		if (p.isPresent()) {
			return p;
		} else {
			return fallback.get(key);
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
		Map<ByteString, BocasValue> inPrimary = primary.get(requested);
		Set<ByteString> askFallback = Sets.difference(requested, inPrimary.keySet()).immutableCopy();
		if (askFallback.isEmpty()) {
			return inPrimary;
		}
		Map<ByteString, BocasValue> inSecondary = fallback.get(askFallback);
		if (inSecondary.isEmpty()) {
			return inPrimary;
		}
		Map<ByteString, BocasValue> total = Maps.newHashMap(inPrimary);
		total.putAll(inSecondary);
		return total;
	}

}
