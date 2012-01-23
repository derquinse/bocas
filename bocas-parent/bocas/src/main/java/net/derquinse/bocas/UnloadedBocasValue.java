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

import java.io.IOException;
import java.io.InputStream;

import com.google.common.annotations.Beta;
import com.google.common.io.InputSupplier;

/**
 * A BOCAS repository value loaded into an in-memory byte array. This class is provided as a support
 * for repository implementations.
 * @author Andres Rodriguez.
 */
@Beta
final class UnloadedBocasValue extends BocasValue {
	/** Payload. */
	private final InputSupplier<InputStream> payload;
	/** Payload size. */
	private final Integer size;

	/** Constructor. */
	UnloadedBocasValue(InputSupplier<InputStream> payload, Integer size) {
		this.payload = checkPayload(payload);
		checkArgument(size == null || size.intValue() >= 0, "Invalid payload size");
		this.size = size;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#getSize()
	 */
	@Override
	public Integer getSize() {
		return size;
	}

	/*
	 * (non-Javadoc)
	 * @see com.google.common.io.InputSupplier#getInput()
	 */
	@Override
	public InputStream getInput() throws IOException {
		return payload.getInput();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#load()
	 */
	@Override
	public LoadedBocasValue load() {
		return loaded(this);
	}
}
