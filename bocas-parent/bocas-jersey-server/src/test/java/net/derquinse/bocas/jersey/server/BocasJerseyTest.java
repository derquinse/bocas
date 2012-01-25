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

import static org.junit.Assert.assertTrue;

import java.util.Set;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasExerciser;
import net.derquinse.bocas.BocasServices;
import net.derquinse.bocas.BocasValue;
import net.derquinse.bocas.jersey.client.BocasClientFactory;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.test.RandomSupport;

import org.junit.Assert;
import org.junit.Test;
import org.testng.internal.annotations.Sets;

import com.google.common.io.ByteStreams;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;

/**
 * Integration test for Jersey Bocas service.
 * @author Andres Rodriguez
 */
public class BocasJerseyTest extends JerseyTest {
	static final Bocas SERVER = BocasServices.memory(); 

	private static LowLevelAppDescriptor descriptor() {
		Set<Class<?>> set = Sets.newHashSet();
		set.add(TestBocasResource.class);
		Class<?>[] classes = new Class<?>[set.size()];
		set.toArray(classes);
		return new LowLevelAppDescriptor.Builder(classes).build();
	}

	public BocasJerseyTest() {
		super(descriptor());
	}
	
	@Test
	public void test() throws Exception {
		Bocas client = BocasClientFactory.create().get(getBaseURI());
		// Thread.sleep(1000);
		// BocasExerciser.exercise(client);
		byte[] data = RandomSupport.getBytes(3072);
		ByteString key = client.put(ByteStreams.newInputStreamSupplier(data));
		assertTrue(SERVER.contains(key));
		assertTrue(client.contains(key));
		BocasValue v = client.get(key).get();
		Assert.assertArrayEquals(data, ByteStreams.toByteArray(v));
	}

	@Test
	public void exercise() throws Exception {
		Bocas client = BocasClientFactory.create().get(getBaseURI());
		BocasExerciser.exercise(client);
	}

}
