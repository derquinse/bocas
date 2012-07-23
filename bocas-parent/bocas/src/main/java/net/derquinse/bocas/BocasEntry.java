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
import net.derquinse.common.meta.MetaProperty;

import com.google.common.annotations.Beta;
import com.google.common.io.InputSupplier;

/**
 * A Bocas repository entry. This class is provided as a support for repository implementations.
 * @author Andres Rodriguez.
 */
@Beta
public abstract class BocasEntry {
	/** Key property. */
	public static final MetaProperty<BocasEntry, ByteString> KEY = new MetaProperty<BocasEntry, ByteString>("key", true) {
		@Override
		public ByteString apply(BocasEntry input) {
			return input.getKey();
		}
	};

	/** Value property. */
	public static final MetaProperty<BocasEntry, BocasValue> VALUE = new MetaProperty<BocasEntry, BocasValue>("value",
			true) {
		@Override
		public BocasValue apply(BocasEntry input) {
			return input.getValue();
		}
	};

	/** Entry key. */
	private final ByteString key;

	private static <T extends InputSupplier<? extends InputStream>> T checkValue(T value) {
		return checkNotNull(value, "The Bocas entry value must be provided");
	}

	/**
	 * Creates a new loaded entry.
	 * @param bytes Entry value payload. The provided byte array MUST NOT be modified after being
	 *          handed to the value, as no defensive copy is performed.
	 * @return The created value.
	 */
	public static LoadedBocasEntry of(byte[] bytes) {
		return new LoadedBocasEntry(BocasValue.of(bytes));
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
		if (value instanceof DirectBocasValue) {
			return new DirectBocasEntry((DirectBocasValue) value);
		}
		@SuppressWarnings("unchecked")
		InputSupplier<InputStream> v = (InputSupplier<InputStream>) value;
		return new UnloadedBocasEntry(new UnloadedBocasValue(v, size));
	}

	/**
	 * Creates a new entry with the provided value.
	 * @param value Entry value.
	 * @return The created entry. If the argument is a loaded value, the result will be a loaded
	 *         entry.
	 * @throws BocasException if unable to read the value payload.
	 */
	public static BocasEntry of(InputSupplier<? extends InputStream> value) {
		return of(value, null);
	}

	/**
	 * Creates a new entry with an in-memory copy of the the provided value.
	 * @param value Entry value.
	 * @return The created entry. Its value is backed by an in-memory array.
	 */
	public static LoadedBocasEntry loaded(InputSupplier<? extends InputStream> value) {
		return new LoadedBocasEntry(BocasValue.loaded(value));
	}

	/**
	 * Creates a new entry with an in-memory copy of the the provided value.
	 * @param value Entry value.
	 * @return The created entry. Its value is backed by an in-memory array.
	 */
	public static LoadedBocasEntry loaded(InputStream value) {
		return new LoadedBocasEntry(BocasValue.loaded(value));
	}

	/**
	 * Creates a new entry with the value loaded into a direct byte buffer.
	 * @param value Entry value.
	 * @return The created entry. Its value is backed by an in-memory array.
	 */
	public static DirectBocasEntry direct(InputSupplier<? extends InputStream> value) {
		return new DirectBocasEntry(BocasValue.direct(value));
	}

	/**
	 * Creates a new entry with the value loaded into a direct byte buffer.
	 * @param value Entry value.
	 * @return The created entry. Its value is backed by an in-memory array.
	 */
	public static DirectBocasEntry direct(InputStream value) {
		return new DirectBocasEntry(BocasValue.direct(value));
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
	public LoadedBocasEntry load() {
		return new LoadedBocasEntry(getValue().load());
	}

	/**
	 * Turns this entry into a direct entry.
	 * @throws BocasException if unable to read the value payload.
	 */
	public DirectBocasEntry direct() {
		return new DirectBocasEntry(getValue().direct());
	}

}
