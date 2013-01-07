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

import java.io.IOException;
import java.util.Map.Entry;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.base.Digests;

import com.google.common.annotations.Beta;
import com.google.common.collect.Maps;

/**
 * A loaded bocas value.
 * @author Andres Rodriguez.
 */
@Beta
public abstract class LoadedBocasValue extends BocasValue {
	/** Constructor. */
	LoadedBocasValue() {
	}

	/** Computes the key of the loaded value. */
	public final ByteString key() {
		try {
			return Digests.sha256(this);
		} catch (IOException e) {
			// The value is loaded, should not happen.
			throw new BocasException(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#load()
	 */
	@Override
	public final LoadedBocasValue toLoaded() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#toEntry()
	 */
	@Override
	public final Entry<ByteString, LoadedBocasValue> toEntry() {
		ByteString key = key();
		return Maps.immutableEntry(key, this);
	}

}
