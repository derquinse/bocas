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

import com.google.common.annotations.Beta;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

/**
 * A Bocas repository value.
 * @author Andres Rodriguez.
 */
@Beta
public abstract class BocasValue implements InputSupplier<InputStream> {
	static <T> T checkPayload(T payload) {
		return checkNotNull(payload, "The Bocas value payload must be provided");
	}

	/**
	 * Creates a new loaded value.
	 * @param bytes Value payload. The provided byte array MUST NOT be modified after being handed to
	 *          the value, as no defensive copy is performed.
	 * @return The created value.
	 */
	public static LoadedBocasValue of(byte[] bytes) {
		return new LoadedBocasValue(bytes);
	}

	/**
	 * Creates a new value with the provided payload.
	 * @param payload Value payload.
	 * @param size Payload size.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static BocasValue of(InputSupplier<? extends InputStream> payload, Integer size) {
		if (payload instanceof BocasValue) {
			return (BocasValue) payload;
		}
		@SuppressWarnings("unchecked")
		InputSupplier<InputStream> p = (InputSupplier<InputStream>) payload;
		return new UnloadedBocasValue(p, size);
	}

	/**
	 * Creates a new value with the provided payload and an unknown size.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static BocasValue of(InputSupplier<? extends InputStream> payload) {
		return of(payload, null);
	}

	/**
	 * Creates a new value with an in-memory copy of the the provided payload.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static LoadedBocasValue loaded(InputSupplier<? extends InputStream> payload) {
		if (payload instanceof BocasValue) {
			return ((BocasValue) payload).load();
		}
		return internalLoaded(payload);
	}

	private static LoadedBocasValue internalLoaded(InputSupplier<? extends InputStream> payload) {
		try {
			byte[] data = ByteStreams.toByteArray(checkPayload(payload));
			return new LoadedBocasValue(data);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Creates a new value with an in-memory copy of the the provided payload.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static LoadedBocasValue loaded(InputStream payload) {
		try {
			byte[] data = ByteStreams.toByteArray(checkPayload(payload));
			return new LoadedBocasValue(data);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Creates a new value loaded into a direct byte buffer.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static DirectBocasValue direct(InputSupplier<? extends InputStream> payload) {
		if (payload instanceof BocasValue) {
			return ((BocasValue) payload).direct();
		}
		return internalDirect(payload);
	}

	private static DirectBocasValue internalDirect(InputSupplier<? extends InputStream> payload) {
		try {
			byte[] data = ByteStreams.toByteArray(checkPayload(payload));
			return new DirectBocasValue(data);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Creates a new value loaded into a direct byte buffer.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static DirectBocasValue direct(InputStream payload) {
		try {
			byte[] data = ByteStreams.toByteArray(checkPayload(payload));
			return new DirectBocasValue(data);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/** Constructor. */
	BocasValue() {
	}

	/** Returns the payload size in bytes (if known). */
	public abstract Integer getSize();

	/** Copies and returns the internal data. */
	public byte[] toByteArray() throws IOException {
		return ByteStreams.toByteArray(this);
	}

	/**
	 * Turns this value into a loaded one.
	 * @throws BocasException if unable to load the payload.
	 */
	public LoadedBocasValue load() {
		return internalLoaded(this);
	}

	/**
	 * Turns this value into a direct one.
	 * @throws BocasException if unable to load the payload.
	 */
	public DirectBocasValue direct() {
		return internalDirect(this);
	}

}
