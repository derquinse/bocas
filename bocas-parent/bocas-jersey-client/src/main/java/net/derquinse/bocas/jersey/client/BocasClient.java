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
import static net.derquinse.bocas.BocasPreconditions.checkLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasHashFunction;
import net.derquinse.bocas.jersey.BocasResources;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.io.Closer;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.MultiPart;
import com.sun.jersey.multipart.MultiPartMediaTypes;

/**
 * Bocas bucket client based on Jersey (JAX-RS).
 * @author Andres Rodriguez.
 */
final class BocasClient implements Bocas {
	/** Hash splitter. */
	private static final Splitter HASH_SPLITTER = Splitter.on(CharMatcher.WHITESPACE).trimResults().omitEmptyStrings();

	/** Bucket resource. */
	private final WebResource resource;
	/** Memory loader to use. */
	private final MemoryByteSourceLoader loader;

	private static ByteString checkKey(ByteString key) {
		return checkNotNull(key, "The object key must be provided");
	}

	private static <T> T checkValue(T value) {
		return checkNotNull(value, "The value to put must be provided");
	}

	private static <T> T checkValues(T values) {
		return checkNotNull(values, "The values to put must be provided");
	}

	private static BocasException exception(Throwable t) {
		return new BocasException(t);
	}

	private static WebResource object(WebResource base, ByteString key) {
		WebResource r = base.path(checkKey(key).toHexString());
		return r;
	}

	BocasClient(WebResource resource, MemoryByteSourceLoader loader) {
		this.resource = checkNotNull(resource, "The bucket resource must be provided");
		this.loader = checkLoader(loader);
	}

	private WebResource object(ByteString key) {
		return object(resource, key);
	}

	@Override
	public BocasHashFunction getHashFunction() {
		try {
			String result = resource.path(BocasResources.HASH).get(String.class);
			if (result != null) {
				List<String> split = Lists.newLinkedList(HASH_SPLITTER.split(result)); // TODO: change in
																																								// guava 15
				if (!split.isEmpty()) {
					return BocasHashFunction.get(split.get(0));
				}
			}
			throw new BocasException("Unable to get hash function");
		} catch (UniformInterfaceException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#close()
	 */
	@Override
	public void close() {
		// Nothing to do.
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contains(net.derquinse.common.base.ByteString)
	 */
	@Override
	public boolean contains(ByteString key) {
		try {
			object(resource.path(BocasResources.CATALOG), key).get(String.class);
			return true;
		} catch (UniformInterfaceException e) {
			if (e.getResponse().getStatus() == Status.NOT_FOUND.getStatusCode()) {
				return false;
			}
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#contained(java.lang.Iterable)
	 */
	@Override
	public Set<ByteString> contained(Iterable<ByteString> keys) {
		MultiMethod m = new MultiMethod(resource.path(BocasResources.CATALOG), keys);
		if (m.isEmpty()) {
			return ImmutableSet.of();
		}
		try {
			final String response = m.call(String.class);
			return ImmutableSet.copyOf(BocasResources.response2List(response));
		} catch (UniformInterfaceException e) {
			if (e.getResponse().getStatus() == Status.NOT_FOUND.getStatusCode()) {
				return ImmutableSet.of();
			}
			throw exception(e);
		}
	}

	private MemoryByteSource load(InputStream is) throws IOException {
		Closer closer = Closer.create();
		try {
			return loader.load(closer.register(is));
		} finally {
			closer.close();
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<ByteSource> get(ByteString key) {
		try {
			final InputStream data = object(key).get(InputStream.class);
			if (data == null) {
				return Optional.absent();
			}
			ByteSource v = load(data);
			return Optional.of(v);
		} catch (UniformInterfaceException e) {
			if (e.getResponse().getStatus() == Status.NOT_FOUND.getStatusCode()) {
				return Optional.absent();
			}
			throw exception(e);
		} catch (IOException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, ByteSource> get(Iterable<ByteString> keys) {
		MultiMethod m = new MultiMethod(resource, keys);
		if (m.isEmpty()) {
			return ImmutableMap.of();
		}
		try {
			FormDataMultiPart data = m.call(FormDataMultiPart.class);
			if (data == null) {
				return ImmutableMap.of();
			}
			Map<ByteString, ByteSource> found = Maps.newHashMap();
			for (Entry<String, List<FormDataBodyPart>> entry : data.getFields().entrySet()) {
				List<FormDataBodyPart> parts = entry.getValue();
				if (parts != null && !parts.isEmpty()) {
					try {
						ByteString key = ByteString.fromHexString(entry.getKey());
						ByteSource v = load(parts.get(0).getEntityAs(InputStream.class));
						found.put(key, v);
					} catch (BocasException e) {
						throw e;
					} catch (IOException e) {
						throw exception(e);
					} catch (RuntimeException e) {
						// ignore result
					}
				}
			}
			return found;
		} catch (UniformInterfaceException e) {
			if (e.getResponse().getStatus() == Status.NOT_FOUND.getStatusCode()) {
				return ImmutableMap.of();
			}
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(com.google.common.io.ByteSource)
	 */
	@Override
	public ByteString put(ByteSource value) {
		try {
			String response = resource.entity(checkValue(value), MediaType.APPLICATION_OCTET_STREAM_TYPE).post(String.class);
			List<ByteString> list = BocasResources.response2List(response);
			if (list.size() != 1) {
				throw new BocasException("Unexpected response");
			}
			return list.get(0);
		} catch (UniformInterfaceException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putAll(java.lang.Iterable)
	 */
	@Override
	public List<ByteString> putAll(Iterable<? extends ByteSource> values) {
		checkValues(values);
		try {
			MultiPart multipart = new MultiPart();
			for (ByteSource object : values) {
				multipart.bodyPart(object, MediaType.APPLICATION_OCTET_STREAM_TYPE);
			}
			String response = resource.entity(multipart, MultiPartMediaTypes.MULTIPART_MIXED_TYPE).post(String.class);
			return BocasResources.response2List(response);
		} catch (UniformInterfaceException e) {
			throw exception(e);
		}
	}

	/** Resource that may get called over GET or POST depending on argument number. */
	private static final class MultiMethod {
		private final WebResource r;
		private final Set<ByteString> keys;
		private final String body;

		MultiMethod(WebResource base, Iterable<ByteString> keys) {
			checkNotNull(keys, "The object keys must be provided");
			this.keys = Sets.newHashSet();
			for (ByteString k : keys) {
				this.keys.add(checkKey(k));
			}
			if (this.keys.size() > 20) {
				this.body = BocasResources.iterable2String(this.keys);
			} else {
				this.body = null;
				for (ByteString k : this.keys) {
					base = base.queryParam(BocasResources.KEY, k.toHexString());
				}
			}
			this.r = base;
		}

		boolean isEmpty() {
			return keys.isEmpty();
		}

		<T> T call(Class<T> type) {
			if (body == null) {
				return r.get(type);
			} else {
				return r.post(type, body);
			}
		}

	}

}
