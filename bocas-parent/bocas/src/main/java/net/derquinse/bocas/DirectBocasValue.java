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
import java.io.InputStream;
import java.nio.ByteBuffer;

import net.derquinse.common.io.DerquinseByteStreams;

import com.google.common.annotations.Beta;
import com.google.common.io.InputSupplier;

/**
 * A Bocas repository value loaded into a direct byte buffer.
 * @author Andres Rodriguez.
 */
@Beta
public final class DirectBocasValue extends BocasValue {
	/** Value size. */
	private final Integer size;
	/** Backing buffer. */
	private final ByteBuffer buffer;
	/** Input supplier. */
	private final InputSupplier<InputStream> supplier;

	/** Constructor. */
	DirectBocasValue(byte[] payload) {
		this.size = checkPayload(payload).length;
		this.buffer = ByteBuffer.allocateDirect(this.size);
		buffer.put(payload).flip();
		this.supplier = DerquinseByteStreams.newInputStreamSupplier(buffer);
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
		return supplier.getInput();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#load()
	 */
	@Override
	public LoadedBocasValue load() {
		return new LoadedBocasValue(toByteArray());
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#toByteArray()
	 */
	@Override
	public byte[] toByteArray() {
		final ByteBuffer b = buffer.slice();
		byte[] copy = new byte[size];
		b.get(copy);
		return copy;
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.BocasValue#direct()
	 */
	@Override
	public DirectBocasValue direct() {
		return this;
	}
}
