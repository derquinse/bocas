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
package net.derquinse.bocas.jersey.server;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.derquinse.bocas.BocasPreconditions.checkLoader;
import static net.derquinse.bocas.jersey.BocasResources.iterable2String;
import static net.derquinse.common.jaxrs.ByteSourceOutput.output;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasException;
import net.derquinse.bocas.jersey.BocasResources;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.MultiPart;
import com.sun.jersey.multipart.MultiPartMediaTypes;

/**
 * Bocas bucket JAX-RS resource. This class can be used as a sub-resource or subclassed to become a
 * root resource.
 * @author Andres Rodriguez.
 */
public class BocasResource {
	/** Target bucket. */
	private final Bocas bocas;
	/** Memory loader to use. */
	private final MemoryByteSourceLoader loader;

	private static WebApplicationException notFound() {
		throw new NotFoundException();
	}

	private static Set<ByteString> setFromQuery(@Nullable List<String> keys) {
		if (keys == null || keys.isEmpty()) {
			throw notFound();
		}
		Set<ByteString> set = Sets.newHashSetWithExpectedSize(keys.size());
		for (String k : keys) {
			try {
				set.add(ByteString.fromHexString(k));
			} catch (RuntimeException e) {
			}
		}
		if (set.isEmpty()) {
			throw notFound();
		}
		return set;
	}

	private static Set<ByteString> setFromBody(@Nullable String body) {
		if (body == null) {
			return ImmutableSet.of();
		}
		try {
			Set<ByteString> set = Sets.newHashSet();
			for (String k : BocasResources.KEY_SPLITTER.split(body)) {
				try {
					set.add(ByteString.fromHexString(k));
				} catch (RuntimeException e) {
				}
			}
			return set;
		} catch (RuntimeException e) {
			throw new BocasException(e);
		}
	}

	/**
	 * Constructor.
	 * @param bocas Repository.
	 * @param loader Memory loader to use.
	 */
	public BocasResource(Bocas bocas, MemoryByteSourceLoader loader) {
		this.bocas = checkNotNull(bocas, "The bocas repository must be provided");
		this.loader = checkLoader(loader);
	}

	/** Bucket existance. */
	@GET
	@Path("bucket")
	public final Response get() {
		return Response.ok().build();
	}

	/** Bucket hash function. */
	@GET
	@Path(BocasResources.HASH)
	@Produces(MediaType.TEXT_PLAIN)
	public final String hash() {
		return bocas.getHashFunction().name();
	}

	/** Transform an id path segment into a key. */
	private ByteString getKey(String id) {
		try {
			return ByteString.fromHexString(id);
		} catch (RuntimeException e) {
			throw notFound();
		}
	}

	/** Evaluate preconditions. */
	private Response evaluate(Request request, ByteString key) {
		ResponseBuilder b = request.evaluatePreconditions(new EntityTag(key.toHexString()));
		if (b != null) {
			return b.build();
		}
		return null;
	}

	/** @see Bocas#get(ByteString) */
	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public final Response getObject(@Context Request request, @PathParam("id") String id) throws IOException {
		ByteString key = getKey(id);
		// Check preconditions
		Response pre = evaluate(request, key);
		if (pre != null) {
			return pre;
		}
		// Get object.
		Optional<ByteSource> optional = bocas.get(key);
		if (!optional.isPresent()) {
			throw notFound();
		}
		ByteSource value = optional.get();
		ResponseBuilder b = Response.ok(output(value), MediaType.APPLICATION_OCTET_STREAM).tag(
				new EntityTag(key.toHexString()));
		if (value instanceof MemoryByteSource) {
			b.header(HttpHeaders.CONTENT_LENGTH, Long.toString(value.size()));
		}
		CacheControl cc = new CacheControl();
		cc.setMaxAge(15552000);
		return b.cacheControl(cc).build();
	}

	/** @see Bocas#get(Iterable) */
	private final Response getObjects(Set<ByteString> requested) {
		if (requested.isEmpty()) {
			throw notFound();
		}
		Map<ByteString, ByteSource> found = bocas.get(requested);
		if (found.isEmpty()) {
			throw notFound();
		}
		FormDataMultiPart entity = new FormDataMultiPart();
		for (Entry<ByteString, ByteSource> entry : found.entrySet()) {
			entity.field(entry.getKey().toHexString(), output(entry.getValue()), MediaType.APPLICATION_OCTET_STREAM_TYPE);
		}
		return Response.ok(entity, MediaType.MULTIPART_FORM_DATA).build();
	}

	/** @see Bocas#get(Iterable) */
	@GET
	@Produces(MediaType.MULTIPART_FORM_DATA)
	public final Response getObjects(@QueryParam(BocasResources.KEY) List<String> keys) {
		return getObjects(setFromQuery(keys));
	}

	/** @see Bocas#get(Iterable) */
	@POST
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.MULTIPART_FORM_DATA)
	public final Response getObjects(String keys) {
		return getObjects(setFromBody(keys));
	}

	/** @see Bocas#contains(ByteString) */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path(BocasResources.CATALOG + "/{id}")
	public final Response containsObject(@Context Request request, @PathParam("id") String id) {
		ByteString key = getKey(id);
		// Check preconditions
		Response pre = evaluate(request, key);
		if (pre != null) {
			return pre;
		}
		// Check presence
		if (bocas.contains(key)) {
			ResponseBuilder b = Response.ok(key.toHexString(), MediaType.TEXT_PLAIN).tag(new EntityTag(key.toHexString()));
			CacheControl cc = new CacheControl();
			cc.setMaxAge(30);
			return b.cacheControl(cc).build();
		}
		throw notFound();
	}

	/** @see Bocas#contained(Iterable) */
	private String containsObjects(Set<ByteString> requested) {
		if (requested.isEmpty()) {
			throw notFound();
		}
		Set<ByteString> found = bocas.contained(requested);
		if (found.isEmpty()) {
			throw notFound();
		}
		return iterable2String(found);
	}

	/** @see Bocas#contained(Iterable) */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path(BocasResources.CATALOG)
	public final String containsObjects(@QueryParam(BocasResources.KEY) List<String> keys) {
		return containsObjects(setFromQuery(keys));
	}

	/** @see Bocas#contained(Iterable) */
	@POST
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	@Path(BocasResources.CATALOG)
	public final String containsObjects(String keys) {
		return containsObjects(setFromBody(keys));
	}

	/** @see Bocas#put(ByteSource) */
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@Produces(MediaType.TEXT_PLAIN)
	public final Response putObject(InputStream stream) throws IOException {
		ByteString created = bocas.put(loader.load(stream));
		String key = created.toHexString();
		return Response.created(URI.create(key)).entity(key).build();
	}

	/** @see Bocas#putAll(Iterable) */
	@POST
	@Consumes(MultiPartMediaTypes.MULTIPART_MIXED)
	@Produces(MediaType.TEXT_PLAIN)
	public final Response putObjects(MultiPart entity) throws IOException {
		checkNotNull(entity);
		List<ByteSource> list = Lists.newLinkedList();
		for (BodyPart part : entity.getBodyParts()) {
			list.add(loader.load(part.getEntityAs(InputStream.class)));
		}
		List<ByteString> created = bocas.putAll(list);
		if (created.isEmpty()) {
			throw notFound();
		}
		return Response.status(Status.CREATED).entity(iterable2String(created)).build();
	}
}
