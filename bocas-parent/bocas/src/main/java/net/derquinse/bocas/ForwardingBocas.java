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

import java.util.List;
import java.util.Map;
import java.util.Set;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingObject;
import com.google.common.io.ByteSource;

/**
 * A bocas bucket that forwards all its method calls to another.
 * @author Andres Rodriguez.
 */
@Beta
public abstract class ForwardingBocas extends ForwardingObject implements Bocas {
	/** Constructor to use by subclasses. */
	protected ForwardingBocas() {
	}

	/** Returns the backing delegate instance that methods are forwarded to. */
	@Override
	protected abstract Bocas delegate();

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#getHashFunction()
	 */
	@Override
	public BocasHashFunction getHashFunction() {
		return delegate().getHashFunction();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#close()
	 */
	@Override
	public void close() {
		delegate().close();
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public boolean contains(ByteString key) {
		return delegate().contains(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public Set<ByteString> contained(Iterable<ByteString> keys) {
		return delegate().contained(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<ByteSource> get(ByteString key) {
		return delegate().get(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, ByteSource> get(Iterable<ByteString> keys) {
		return delegate().get(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(com.google.common.io.ByteSource)
	 */
	@Override
	public ByteString put(ByteSource value) {
		return delegate().put(value);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putAll(java.lang.Iterable)
	 */
	@Override
	public List<ByteString> putAll(Iterable<? extends ByteSource> values) {
		return delegate().putAll(values);
	}
}
