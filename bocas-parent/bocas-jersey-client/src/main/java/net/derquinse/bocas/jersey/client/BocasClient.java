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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.BocasValue;
import net.derquinse.bocas.jersey.BocasResources;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.util.zip.MaybeCompressed;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.MultiPart;
import com.sun.jersey.multipart.MultiPartMediaTypes;

/**
 * Bocas repository client based on Jersey (JAX-RS).
 * @author Andres Rodriguez.
 */
final class BocasClient implements Bocas {
	/** Root resource. */
	private final WebResource resource;

	private static ByteString checkKey(ByteString key) {
		return checkNotNull(key, "The object key must be provided");
	}

	private static <T> T checkObject(T object) {
		return checkNotNull(object, "The object to put must be provided");
	}

	private static <T> T checkObjects(T objects) {
		return checkNotNull(objects, "The objects to put must be provided");
	}

	private static BocasException exception(Throwable t) {
		return new BocasException(t);
	}

	private static WebResource object(WebResource base, ByteString key) {
		WebResource r = base.path(checkKey(key).toHexString());
		return r;
	}

	BocasClient(WebResource resource) {
		this.resource = checkNotNull(resource, "The root resource must be provided");
	}

	private WebResource object(ByteString key) {
		return object(resource, key);
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
			if (e.getResponse().getClientResponseStatus() == Status.NOT_FOUND) {
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
			if (e.getResponse().getClientResponseStatus() == Status.NOT_FOUND) {
				return ImmutableSet.of();
			}
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(net.derquinse.common.base.ByteString)
	 */
	@Override
	public Optional<BocasValue> get(ByteString key) {
		try {
			final byte[] data = object(key).get(byte[].class);
			if (data == null) {
				return Optional.absent();
			}
			BocasValue v = BocasValue.of(data);
			return Optional.of(v);
		} catch (UniformInterfaceException e) {
			if (e.getResponse().getClientResponseStatus() == Status.NOT_FOUND) {
				return Optional.absent();
			}
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#get(java.lang.Iterable)
	 */
	@Override
	public Map<ByteString, BocasValue> get(Iterable<ByteString> keys) {
		MultiMethod m = new MultiMethod(resource, keys);
		if (m.isEmpty()) {
			return ImmutableMap.of();
		}
		try {
			FormDataMultiPart data = m.call(FormDataMultiPart.class);
			if (data == null) {
				return ImmutableMap.of();
			}
			Map<ByteString, BocasValue> found = Maps.newHashMap();
			for (Entry<String, List<FormDataBodyPart>> entry : data.getFields().entrySet()) {
				List<FormDataBodyPart> parts = entry.getValue();
				if (parts != null && !parts.isEmpty()) {
					try {
						ByteString key = ByteString.fromHexString(entry.getKey());
						BocasValue v = BocasValue.of(parts.get(0).getEntityAs(byte[].class));
						found.put(key, v);
					} catch (BocasException e) {
						throw e;
					} catch (RuntimeException e) {
						// ignore result
					}
				}
			}
			return found;
		} catch (UniformInterfaceException e) {
			if (e.getResponse().getClientResponseStatus() == Status.NOT_FOUND) {
				return ImmutableMap.of();
			}
			throw exception(e);
		}
	}

	private ByteString put(byte[] object) {
		try {
			String response = resource.entity(object, MediaType.APPLICATION_OCTET_STREAM_TYPE).post(String.class);
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
	 * @see net.derquinse.bocas.Bocas#put(com.google.common.io.InputSupplier)
	 */
	@Override
	public ByteString put(InputSupplier<? extends InputStream> object) {
		try {
			return put(ByteStreams.toByteArray(checkObject(object)));
		} catch (IOException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#put(java.io.InputStream)
	 */
	@Override
	public ByteString put(InputStream object) {
		try {
			return put(ByteStreams.toByteArray(checkObject(object)));
		} catch (IOException e) {
			throw exception(e);
		}
	}

	private List<ByteString> put(List<byte[]> objects) {
		try {
			MultiPart multipart = new MultiPart();
			for (byte[] object : objects) {
				multipart.bodyPart(object, MediaType.APPLICATION_OCTET_STREAM_TYPE);
			}
			String response = resource.entity(multipart, MultiPartMediaTypes.MULTIPART_MIXED_TYPE).post(String.class);
			return BocasResources.response2List(response);
		} catch (UniformInterfaceException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putSuppliers(java.util.List)
	 */
	@Override
	public List<ByteString> putSuppliers(List<? extends InputSupplier<? extends InputStream>> objects) {
		checkObjects(objects);
		try {
			List<byte[]> arrays = Lists.newArrayListWithExpectedSize(objects.size());
			for (InputSupplier<? extends InputStream> object : objects) {
				arrays.add(ByteStreams.toByteArray(checkObject(object)));
			}
			return put(arrays);
		} catch (IOException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putStreams(java.util.List)
	 */
	@Override
	public List<ByteString> putStreams(List<? extends InputStream> objects) {
		checkObjects(objects);
		try {
			List<byte[]> arrays = Lists.newArrayListWithExpectedSize(objects.size());
			for (InputStream object : objects) {
				arrays.add(ByteStreams.toByteArray(checkObject(object)));
			}
			return put(arrays);
		} catch (IOException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putZip(java.io.InputStream)
	 */
	@Override
	public Map<String, ByteString> putZip(InputStream object) {
		try {
			return putZip(ByteStreams.toByteArray(checkObject(object)));
		} catch (IOException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putZip(com.google.common.io.InputSupplier)
	 */
	@Override
	public Map<String, ByteString> putZip(InputSupplier<? extends InputStream> object) {
		try {
			return putZip(ByteStreams.toByteArray(checkObject(object)));
		} catch (IOException e) {
			throw exception(e);
		}
	}

	private Map<String, ByteString> putZip(byte[] object) {
		try {
			String response = resource.path(BocasResources.ZIP).entity(object, MediaType.APPLICATION_OCTET_STREAM_TYPE)
					.post(String.class);
			return BocasResources.response2zip(response);
		} catch (UniformInterfaceException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putZipAndGZip(java.io.InputStream)
	 */
	@Override
	public Map<String, MaybeCompressed<ByteString>> putZipAndGZip(InputStream object) {
		try {
			return putZipAndGZip(ByteStreams.toByteArray(checkObject(object)));
		} catch (IOException e) {
			throw exception(e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see net.derquinse.bocas.Bocas#putZipAndGZip(com.google.common.io.InputSupplier)
	 */
	@Override
	public Map<String, MaybeCompressed<ByteString>> putZipAndGZip(InputSupplier<? extends InputStream> object) {
		try {
			return putZipAndGZip(ByteStreams.toByteArray(checkObject(object)));
		} catch (IOException e) {
			throw exception(e);
		}
	}

	private Map<String, MaybeCompressed<ByteString>> putZipAndGZip(byte[] object) {
		try {
			String response = resource.path(BocasResources.ZIPGZIP).entity(object, MediaType.APPLICATION_OCTET_STREAM_TYPE)
					.post(String.class);
			return BocasResources.response2czip(response);
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
