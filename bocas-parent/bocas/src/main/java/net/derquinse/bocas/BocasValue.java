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
import java.util.Map.Entry;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

/**
 * A bocas value. A value may be loaded (either in the JVM heap or a direct buffer) or not.
 * @author Andres Rodriguez.
 */
@Beta
public abstract class BocasValue implements InputSupplier<InputStream> {

	/** Value to loaded value. */
	static final Function<BocasValue, LoadedBocasValue> LOADER = new Function<BocasValue, LoadedBocasValue>() {
		public LoadedBocasValue apply(BocasValue input) {
			return input.toLoaded();
		}
	};

	static <T> T checkPayload(T payload) {
		return checkNotNull(payload, "The bocas value payload must be provided");
	}

	/**
	 * Creates a new heap value.
	 * @param bytes Value payload. The provided byte array MUST NOT be modified after being handed to
	 *          the value, as no defensive copy is performed.
	 * @return The created value.
	 */
	public static HeapBocasValue of(byte[] bytes) {
		return new HeapBocasValue(bytes);
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
	 * Creates a list of values from the provided payloads.
	 * @param payloads Payloads.
	 * @return The created values.
	 * @throws BocasException if unable to load any of the payloads.
	 */
	public static ImmutableList<BocasValue> list(Iterable<? extends InputSupplier<? extends InputStream>> payloads) {
		return ImmutableList.copyOf(Iterables.transform(payloads, value()));
	}

	/**
	 * Creates a loaded value with the provided payload. Unless a direct value is provided, a heap
	 * value is returned.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static LoadedBocasValue loaded(InputSupplier<? extends InputStream> payload) {
		if (payload instanceof BocasValue) {
			return ((BocasValue) payload).toLoaded();
		}
		return internalHeap(payload);
	}

	private static HeapBocasValue internalHeap(InputSupplier<? extends InputStream> payload) {
		try {
			byte[] data = ByteStreams.toByteArray(checkPayload(payload));
			return new HeapBocasValue(data);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Creates a list of loaded values from the provided payloads.
	 * @param payloads Payloads.
	 * @return The created values.
	 * @throws BocasException if unable to load any of the payloads.
	 */
	public static ImmutableList<LoadedBocasValue> loaded(Iterable<? extends InputSupplier<? extends InputStream>> payloads) {
		return ImmutableList.copyOf(Iterables.transform(payloads, loaded()));
	}

	/**
	 * Creates a heap value with the provided payload.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static HeapBocasValue heap(InputSupplier<? extends InputStream> payload) {
		if (payload instanceof BocasValue) {
			return ((BocasValue) payload).toHeap();
		}
		return internalHeap(payload);
	}

	/**
	 * Creates a new heap value with the provided payload.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static HeapBocasValue heap(InputStream payload) {
		try {
			byte[] data = ByteStreams.toByteArray(checkPayload(payload));
			return new HeapBocasValue(data);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Creates a list of heap values from the provided payloads.
	 * @param payloads Payloads.
	 * @return The created values.
	 * @throws BocasException if unable to load any of the payloads.
	 */
	public static ImmutableList<HeapBocasValue> heap(Iterable<? extends InputSupplier<? extends InputStream>> payloads) {
		return ImmutableList.copyOf(Iterables.transform(payloads, heap()));
	}

	/**
	 * Creates a list of heap values from the provided payloads.
	 * @param payloads Payloads.
	 * @return The created values.
	 * @throws BocasException if unable to load any of the payloads.
	 */
	public static ImmutableList<HeapBocasValue> streams2Heap(Iterable<InputStream> payloads) {
		return ImmutableList.copyOf(Iterables.transform(payloads, stream2Heap()));
	}

	/**
	 * Creates a new value loaded into a direct byte buffer.
	 * @param payload Value payload.
	 * @return The created value.
	 * @throws BocasException if unable to load the payload.
	 */
	public static DirectBocasValue direct(InputSupplier<? extends InputStream> payload) {
		if (payload instanceof BocasValue) {
			return ((BocasValue) payload).toDirect();
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

	/**
	 * Creates a list of direct values from the provided payloads.
	 * @param payloads Payloads.
	 * @return The created values.
	 * @throws BocasException if unable to load any of the payloads.
	 */
	public static ImmutableList<DirectBocasValue> direct(Iterable<? extends InputSupplier<? extends InputStream>> payloads) {
		return ImmutableList.copyOf(Iterables.transform(payloads, direct()));
	}

	/**
	 * Creates a list of direct values from the provided payloads.
	 * @param payloads Payloads.
	 * @return The created values.
	 * @throws BocasException if unable to load any of the payloads.
	 */
	public static ImmutableList<DirectBocasValue> streams2Direct(Iterable<InputStream> payloads) {
		return ImmutableList.copyOf(Iterables.transform(payloads, stream2Direct()));
	}

	/**
	 * Turn a collection of payloads into a list of entries. The iteration order of the list is the
	 * same as the input argument.
	 * @throws BocasException if unable to load any of the payloads.
	 */
	public static ImmutableList<Entry<ByteString, LoadedBocasValue>> entryList(
			Iterable<? extends InputSupplier<? extends InputStream>> payloads) {
		ImmutableList.Builder<Entry<ByteString, LoadedBocasValue>> b = ImmutableList.builder();
		for (InputSupplier<? extends InputStream> payload : payloads) {
			b.add(loaded(payload).toEntry());
		}
		return b.build();
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
	 * Turns this value into a loaded one. Unless the value is a direct value, a heap one is returned.
	 * @throws BocasException if unable to load the payload.
	 */
	public LoadedBocasValue toLoaded() {
		return internalHeap(this);
	}

	/**
	 * Turns this value into a heap one.
	 * @throws BocasException if unable to load the payload.
	 */
	public HeapBocasValue toHeap() {
		return internalHeap(this);
	}

	/**
	 * Turns this value into a direct one.
	 * @throws BocasException if unable to load the payload.
	 */
	public DirectBocasValue toDirect() {
		return internalDirect(this);
	}

	/**
	 * Turns this value into a loaded entry.
	 * @throws BocasException if unable to load the payload.
	 */
	public Entry<ByteString, LoadedBocasValue> toEntry() {
		return toLoaded().toEntry();
	}

	// Functions

	/** Returns the input supplier to value function. */
	public static Function<InputSupplier<? extends InputStream>, BocasValue> value() {
		return Supplier2Value.INSTANCE;
	}

	/** Supplier to value function. */
	private enum Supplier2Value implements Function<InputSupplier<? extends InputStream>, BocasValue> {
		INSTANCE;

		public BocasValue apply(InputSupplier<? extends InputStream> input) {
			return of(input);
		}
	}

	/** Returns the supplier to loaded value function. */
	public static Function<InputSupplier<? extends InputStream>, LoadedBocasValue> loaded() {
		return Supplier2LoadedValue.INSTANCE;
	};

	/** Supplier to loaded value function. */
	private enum Supplier2LoadedValue implements Function<InputSupplier<? extends InputStream>, LoadedBocasValue> {
		INSTANCE;

		public LoadedBocasValue apply(InputSupplier<? extends InputStream> input) {
			return loaded(input);
		}
	};

	/** Returns the stream to heap value function. */
	public static Function<InputStream, HeapBocasValue> stream2Heap() {
		return Stream2Heap.INSTANCE;
	};

	/** Stream to heap value function. */
	private enum Stream2Heap implements Function<InputStream, HeapBocasValue> {
		INSTANCE;

		public HeapBocasValue apply(InputStream input) {
			return heap(input);
		}
	};

	/** Returns the supplier to heap value function. */
	public static Function<InputSupplier<? extends InputStream>, HeapBocasValue> heap() {
		return Supplier2HeapValue.INSTANCE;
	};

	/** Supplier to heap value function. */
	private enum Supplier2HeapValue implements Function<InputSupplier<? extends InputStream>, HeapBocasValue> {
		INSTANCE;

		public HeapBocasValue apply(InputSupplier<? extends InputStream> input) {
			return heap(input);
		}
	};

	/** Returns the supplier to direct value function. */
	public static Function<InputSupplier<? extends InputStream>, DirectBocasValue> direct() {
		return Supplier2DirectValue.INSTANCE;
	};

	/** Supplier to loaded value function. */
	private enum Supplier2DirectValue implements Function<InputSupplier<? extends InputStream>, DirectBocasValue> {
		INSTANCE;

		public DirectBocasValue apply(InputSupplier<? extends InputStream> input) {
			return direct(input);
		}
	};

	/** Returns the stream to direct value function. */
	public static Function<InputStream, DirectBocasValue> stream2Direct() {
		return Stream2Direct.INSTANCE;
	};

	/** Stream to direct value function. */
	private enum Stream2Direct implements Function<InputStream, DirectBocasValue> {
		INSTANCE;

		public DirectBocasValue apply(InputStream input) {
			return direct(input);
		}
	};

}
