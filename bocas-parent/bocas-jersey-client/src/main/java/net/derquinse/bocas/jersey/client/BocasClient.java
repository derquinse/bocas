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
package net.derquinse.bocas.jersey.client;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasValue;
import net.derquinse.bocas.jersey.BocasResources;
import net.derquinse.common.base.ByteString;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;

/**
 * Bocas repository client based on Jersey (JAX-RS).
 * @author Andres Rodriguez.
 */
final class BocasClient implements Bocas {
	/** Root resource. */
	private final WebResource resource;

	private static String checkKey(ByteString key) {
		return checkNotNull(key, "The object key must be provided").toHexString();
	}

	private static BocasException exception(ClientResponse r) {
		return exception(new UniformInterfaceException(r));
	}

	private static BocasException exception(Throwable t) {
		return new BocasException(t);
	}

	private String iterable2String(Iterable<ByteString> keys) {
		checkNotNull(keys, "The object key must be provided");
		StringBuilder b = new StringBuilder();
		for (ByteString key : keys) {
			b.append(checkKey(key)).append('\n');
		}
		return b.toString();
	}

	BocasClient(WebResource resource) {
		this.resource = checkNotNull(resource, "The root resource must be provided");
	}

	@Override
	public boolean contains(ByteString key) {
		ClientResponse r = resource.path(checkKey(key)).head();
		Status s = r.getClientResponseStatus();
		if (s == Status.OK) {
			return true;
		}
		if (s == Status.NOT_FOUND) {
			return false;
		}
		throw exception(r);
	}

	@Override
	public Set<ByteString> contained(Iterable<ByteString> keys) {
		final String request = iterable2String(keys);
		final String response;
		try {
			response = resource.path(BocasResources.CATALOG).post(String.class, request);
		} catch (UniformInterfaceException e) {
			if (e.getResponse().getClientResponseStatus() == Status.NOT_FOUND) {
				return ImmutableSet.of();
			}
			throw exception(e);
		}
		if (response == null) {
			return ImmutableSet.of();
		}
		try {
			Set<ByteString> set = Sets.newHashSet();
			for (String k : BocasResources.KEY_SPLITTER.split(response)) {
				set.add(ByteString.fromHexString(k));
			}
			return set;
		} catch (RuntimeException e) {
			throw exception(e);
		}
	}

	@Override
	public Optional<BocasValue> get(ByteString key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ByteString, BocasValue> get(Iterable<ByteString> keys) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteString put(InputSupplier<? extends InputStream> object) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ByteString put(InputStream object) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ByteString> putSuppliers(List<? extends InputSupplier<? extends InputStream>> objects) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<ByteString> putStreams(List<? extends InputStream> objects) {
		// TODO Auto-generated method stub
		return null;
	}

}
