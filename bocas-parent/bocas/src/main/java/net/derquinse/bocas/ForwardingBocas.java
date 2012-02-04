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

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.derquinse.common.base.ByteString;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.collect.ForwardingObject;
import com.google.common.io.InputSupplier;

/**
 * A BOCAS repository that forwards all its method calls to another.
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
	public Optional<BocasValue> get(ByteString key) {
		return delegate().get(key);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, BocasValue> get(Iterable<ByteString> keys) {
		return delegate().get(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(com.google.common.io.InputSupplier)
	 */
	@Override
	public ByteString put(InputSupplier<? extends InputStream> object) {
		return delegate().put(object);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(java.io.InputStream)
	 */
	@Override
	public ByteString put(InputStream object) {
		return delegate().put(object);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putAll(java.util.List)
	 */
	@Override
	public List<ByteString> putSuppliers(List<? extends InputSupplier<? extends InputStream>> objects) {
		return delegate().putSuppliers(objects);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putStreams(java.util.List)
	 */
	@Override
	public List<ByteString> putStreams(List<? extends InputStream> objects) {
		return delegate().putStreams(objects);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putZip(java.io.InputStream)
	 */
	@Override
	public Map<String, ByteString> putZip(InputStream object) {
		return delegate().putZip(object);
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putZip(com.google.common.io.InputSupplier)
	 */
	@Override
	public Map<String, ByteString> putZip(InputSupplier<? extends InputStream> object) {
		return delegate().putZip(object);
	}

}
