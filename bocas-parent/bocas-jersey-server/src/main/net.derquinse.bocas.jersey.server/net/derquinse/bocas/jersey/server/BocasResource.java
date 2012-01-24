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
import static net.derquinse.bocas.jersey.BocasResources.KEY_SPLITTER;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasValue;
import net.derquinse.bocas.jersey.BocasResources;
import net.derquinse.common.base.ByteString;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.multipart.BodyPart;
import com.sun.jersey.multipart.BodyPartEntity;
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

	private static StreamingOutput value2Output(final BocasValue value) {
		return new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException, WebApplicationException {
				ByteStreams.copy(value, output);
			}
		};
	}

	private static WebApplicationException notFound() {
		throw new NotFoundException();
	}

	/**
	 * Constructor.
	 * @param bocas Repository.
	 */
	public BocasResource(Bocas bocas) {
		this.bocas = checkNotNull(bocas, "The bocas respository must be provided");
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

	private static Set<ByteString> getKeys(@Nullable String query) {
		if (query == null) {
			return ImmutableSet.of();
		}
		Set<ByteString> set = Sets.newHashSet();
		for (String s : KEY_SPLITTER.split(query)) {
			try {
				set.add(ByteString.fromHexString(s));
			} catch (RuntimeException e) {
			}
		}
		return set;
	}

	/** @see Bocas#get(Iterable) */
	@POST
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.MULTIPART_FORM_DATA)
	public final Response getObjects(String query) {
		Set<ByteString> requested = getKeys(query);
		if (requested.isEmpty()) {
			throw notFound();
		}
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
	@HEAD
	@Path("{id}")
	public final Response containsObject(@PathParam("id") String id) {
		ByteString key;
		try {
			key = ByteString.fromHexString(id);
		} catch (RuntimeException e) {
			throw notFound();
		}
		if (bocas.contains(key)) {
			return Response.ok().build();
			// TODO: cache control
		}
		throw notFound();
	}

	private String collection2String(Collection<ByteString> keys) {
		StringBuilder b = new StringBuilder();
		for (ByteString key : keys) {
			b.append(key.toHexString()).append('\n');
		}
		return b.toString();
	}

	/** @see Bocas#contained(Iterable) */
	@POST
	@Path(BocasResources.CATALOG)
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public final String containsObjects(String query) {
		Set<ByteString> requested = getKeys(query);
		if (requested.isEmpty()) {
			throw notFound();
		}
		Set<ByteString> found = bocas.contained(requested);
		if (found.isEmpty()) {
			throw notFound();
		}
		return collection2String(found);
	}

	/** @see Bocas#put(InputStream) */
	@POST
	@Path(BocasResources.FUNNEL)
	@Consumes(MediaType.APPLICATION_OCTET_STREAM)
	@Produces(MediaType.TEXT_PLAIN)
	public final Response putObject(InputStream stream) {
		ByteString created = bocas.put(stream);
		String key = created.toHexString();
		return Response.created(URI.create(key)).entity(key).build();
	}

	/** @see Bocas#putStreams(java.util.List) */
	@POST
	@Path(BocasResources.FUNNEL)
	@Consumes(MultiPartMediaTypes.MULTIPART_MIXED)
	@Produces(MediaType.TEXT_PLAIN)
	public final Response putObjects(MultiPart entity) {
		checkNotNull(entity);
		List<InputStream> list = Lists.newLinkedList();
		for (BodyPart part : entity.getBodyParts()) {
			list.add(part.getEntityAs(BodyPartEntity.class).getInputStream());
		}
		List<ByteString> created = bocas.putStreams(list);
		if (created.isEmpty()) {
			throw notFound();
		}
		return Response.status(Status.CREATED).entity(collection2String(created)).build();
	}

}
