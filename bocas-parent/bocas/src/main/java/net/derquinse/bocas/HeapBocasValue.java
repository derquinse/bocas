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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.annotations.Beta;

/**
 * A bocas value loaded into an in-heap byte array.
 * @author Andres Rodriguez.
 */
@Beta
public final class HeapBocasValue extends LoadedBocasValue {
	/** Payload. */
	private final byte[] payload;

	/** Constructor. */
	HeapBocasValue(byte[] payload) {
		this.payload = checkPayload(payload);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#getSize()
	 */
	@Override
	public Integer getSize() {
		return payload.length;
	}

	/*
	 * (non-Javadoc)
	 * @see com.google.common.io.InputSupplier#getInput()
	 */
	@Override
	public InputStream getInput() throws IOException {
		return new ByteArrayInputStream(payload);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#heap()
	 */
	@Override
	public HeapBocasValue toHeap() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#toByteArray()
	 */
	@Override
	public byte[] toByteArray() throws IOException {
		byte[] copy = new byte[payload.length];
		System.arraycopy(payload, 0, copy, 0, payload.length);
		return copy;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#direct()
	 */
	@Override
	public DirectBocasValue toDirect() {
		return new DirectBocasValue(payload);
	}

}
