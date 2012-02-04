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
import static net.derquinse.bocas.jersey.BocasResources.iterable2String;
import static net.derquinse.bocas.jersey.BocasResources.value2Output;
import static net.derquinse.bocas.jersey.BocasResources.zip2response;

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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasValue;
import net.derquinse.bocas.jersey.BocasResources;
import net.derquinse.common.base.ByteString;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.MultiPart;
import com.sun.jersey.multipart.MultiPartMediaTypes;

/**
 * Bocas repository JAX-RS resource. This class can be used as a sub-resource or subclassed to
 * become a root resource.
 * @author Andres Rodriguez.
 */
public class BocasResource {
	/** Repository. */
	private final Bocas bocas;

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

	/**
	 * Constructor.
	 * @param bocas Repository.
	 */
	public BocasResource(Bocas bocas) {
		this.bocas = checkNotNull(bocas, "The bocas repository must be provided");
	}

	/** @see Bocas#get(ByteString) */
	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public final Response getObject(@PathParam("id") String id) {
		ByteString key;
		try {
			key = ByteString.fromHexString(id);
		} catch (RuntimeException e) {
			throw notFound();
		}
		Optional<BocasValue> optional = bocas.get(key);
		if (!optional.isPresent()) {
			throw notFound();
		}
		BocasValue value = optional.get();
		ResponseBuilder b = Response.ok(value2Output(value), MediaType.APPLICATION_OCTET_STREAM);
		Integer size = value.getSize();
		if (size != null) {
			b.header(HttpHeaders.CONTENT_LENGTH, value.toString());
		}
		// TODO: cache control
		return b.build();
	}

	/** @see Bocas#get(Iterable) */
	@GET
	@Produces(MediaType.MULTIPART_FORM_DATA)
	public final Response getObjects(@QueryParam(BocasResources.KEY) List<String> keys) {
		Set<ByteString> requested = setFromQuery(keys);
		Map<ByteString, BocasValue> found = bocas.get(requested);
		if (found.isEmpty()) {
			throw notFound();
		}
		FormDataMultiPart entity = new FormDataMultiPart();
		for (Entry<ByteString, BocasValue> entry : found.entrySet()) {
			entity.field(entry.getKey().toHexString(), value2Output(entry.getValue()),
					MediaType.APPLICATION_OCTET_STREAM_TYPE);
		}
		return Response.ok(entity, MediaType.MULTIPART_FORM_DATA).build();
	}

	/** @see Bocas#contains(ByteString) */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path(BocasResources.CATALOG + "/{id}")
	public final String containsObject(@PathParam("id") String id) {
		ByteString key;
		try {
			key = ByteString.fromHexString(id);
		} catch (RuntimeException e) {
			throw notFound();
		}
		if (bocas.contains(key)) {
			return id;
			// TODO: etag
		}
		throw notFound();
	}

	/** @see Bocas#contained(Iterable) */
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path(BocasResources.CATALOG)
	public final String containsObjects(@QueryParam(BocasResources.KEY) List<String> keys) {
		Set<ByteString> requested = setFromQuery(keys);
		Set<ByteString> found = bocas.contained(requested);
		if (found.isEmpty()) {
			throw notFound();
		}
		return iterable2String(found);
	}

	/** @see Bocas#put(InputStream) */
	@POST
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@Produces(MediaType.TEXT_PLAIN)
	public final Response putObject(InputStream stream) {
		ByteString created = bocas.put(stream);
		String key = created.toHexString();
		return Response.created(URI.create(key)).entity(key).build();
	}

	/** @see Bocas#putStreams(java.util.List) */
	@POST
	@Consumes(MultiPartMediaTypes.MULTIPART_MIXED)
	@Produces(MediaType.TEXT_PLAIN)
	public final Response putObjects(MultiPart entity) {
		checkNotNull(entity);
		List<InputStream> list = Lists.newLinkedList();
		for (BodyPart part : entity.getBodyParts()) {
			list.add(part.getEntityAs(InputStream.class));
		}
		List<ByteString> created = bocas.putStreams(list);
		if (created.isEmpty()) {
			throw notFound();
		}
		return Response.status(Status.CREATED).entity(iterable2String(created)).build();
	}

	/** @see Bocas#put(InputStream) */
	@POST
	@Path(BocasResources.ZIP)
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@Produces(MediaType.TEXT_PLAIN)
	public final Response putZip(InputStream stream) {
		return Response.ok(zip2response(bocas.putZip(stream))).build();
	}

}
