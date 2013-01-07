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

import com.google.common.base.Objects;

/**
 * A bucket key object.
 * @author Andres Rodriguez.
 */
final class BucketKey {
	/** Bucket compared by identity. */
	private final Bocas bocas;
	/** Entry key. */
	private final ByteString key;
	/** Hash code. */
	private final int hash;

	/** Constructor. */
	BucketKey(Bocas bocas, ByteString key) {
		this.bocas = checkNotNull(bocas);
		this.key = checkNotNull(key);
		this.hash = Objects.hashCode(bocas, key);
	}
	
	/** Returns the entry key. */
	ByteString getKey() {
		return key;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BucketKey) {
			BucketKey k = (BucketKey) obj;
			return hash == k.hash && bocas == k.bocas && key.equals(k.key);
		}
		return false;
	}
}
