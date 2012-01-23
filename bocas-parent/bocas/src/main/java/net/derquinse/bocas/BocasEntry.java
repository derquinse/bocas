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
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

/**
 * A BOCAS repository entry. This class is provided as a support for repository implementations.
 * @author Andres Rodriguez.
 */
@Beta
public final class BocasEntry {
	private final ByteString key;
	private final InputSupplier<InputStream> value;

	private static InputSupplier<? extends InputStream> check(InputSupplier<? extends InputStream> value) {
		return checkNotNull(value, "The BOCAS entry value must be provided");
	}

	/**
	 * Creates a new entry with the provided value.
	 * @param value Entry value.
	 * @return The created entry. Its value is the same provided input supplier.
	 */
	public static BocasEntry of(InputSupplier<? extends InputStream> value) {
		try {
			ByteString key = Digests.sha256(check(value));
			return new BocasEntry(key, value);
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Creates a new entry with an in-memory copy of the the provided value.
	 * @param value Entry value.
	 * @return The created entry. Its value is backed by an in-memory array.
	 */
	public static BocasEntry loaded(InputSupplier<? extends InputStream> value) {
		try {
			byte[] data = ByteStreams.toByteArray(check(value));
			return of(ByteStreams.newInputStreamSupplier(data));
		} catch (IOException e) {
			throw new BocasException(e);
		}
	}

	/** Constructor. */
	@SuppressWarnings("unchecked")
	private BocasEntry(ByteString key, InputSupplier<? extends InputStream> value) {
		this.key = key;
		this.value = (InputSupplier<InputStream>) value;
	}

	/** Returns the entry key. */
	public ByteString getKey() {
		return key;
	}

	/** Returns the entry value. */
	public InputSupplier<InputStream> getValue() {
		return value;
	}
}
