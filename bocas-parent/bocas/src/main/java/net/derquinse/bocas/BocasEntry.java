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

import java.io.IOException;
import java.io.InputStream;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.base.Digests;

import com.google.common.annotations.Beta;
import com.google.common.io.InputSupplier;

/**
 * A BOCAS repository entry. This class is provided as a support for repository implementations.
 * @author Andres Rodriguez.
 */
@Beta
public abstract class BocasEntry {
	/** Entry key. */
	private final ByteString key;

	private static <T extends InputSupplier<? extends InputStream>> T checkValue(T value) {
		return checkNotNull(value, "The BOCAS entry value must be provided");
	}

	/**
	 * Creates a new entry with the provided value.
	 * @param value Entry value.
	 * @param size Payload size.
	 * @return The created entry. If the argument is a loaded value, the result will be a loaded
	 *         entry.
	 * @throws BocasException if unable to read the value payload.
	 */
	public static BocasEntry of(InputSupplier<? extends InputStream> value, Integer size) {
		if (value instanceof LoadedBocasValue) {
			return new LoadedBocasEntry((LoadedBocasValue) value);
		}
		return new UnloadedBocasEntry(BocasValue.of(value, size));
	}

	/**
	 * Creates a new entry with the provided value.
	 * @param value Entry value.
	 * @return The created entry. If the argument is a loaded value, the result will be a loaded
	 *         entry.
	 * @throws BocasException if unable to read the value payload.
	 */
	public static BocasEntry of(InputSupplier<? extends InputStream> value) {
		if (value instanceof LoadedBocasValue) {
			return new LoadedBocasEntry((LoadedBocasValue) value);
		}
		return new UnloadedBocasEntry(BocasValue.of(value));
	}

	/**
	 * Creates a new entry with an in-memory copy of the the provided value.
	 * @param value Entry value.
	 * @return The created entry. Its value is backed by an in-memory array.
	 */
	public static LoadedBocasEntry loaded(InputSupplier<? extends InputStream> value) {
		return new LoadedBocasEntry(BocasValue.loaded(value));
	}

	/** Constructor. */
	BocasEntry(InputSupplier<? extends InputStream> value) {
		try {
			this.key = Digests.sha256(checkValue(value));
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/** Returns the entry key. */
	public final ByteString getKey() {
		return key;
	}

	/** Returns the entry value. */
	public abstract BocasValue getValue();

	/**
	 * Turns this entry into a loaded entry.
	 * @throws BocasException if unable to read the value payload.
	 */
	public abstract LoadedBocasEntry load();
}
