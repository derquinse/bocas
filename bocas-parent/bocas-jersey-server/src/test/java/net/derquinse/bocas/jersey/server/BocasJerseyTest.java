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

import static net.derquinse.bocas.BocasHashFunction.sha256;
import static net.derquinse.common.io.MemoryByteSourceLoader.get;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasExerciser;
import net.derquinse.bocas.BocasService;
import net.derquinse.bocas.BocasServices;
import net.derquinse.bocas.jersey.client.BocasClientFactory;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.test.RandomSupport;

import org.junit.Assert;
import org.junit.Test;
import org.testng.internal.annotations.Sets;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSource;
import com.sun.jersey.test.framework.AppDescriptor;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.LowLevelAppDescriptor;

/**
 * Integration test for Jersey Bocas service.
 * @author Andres Rodriguez
 */
public class BocasJerseyTest extends JerseyTest {
	static final BocasService SERVER = BocasServices.shared(BocasServices.memoryBucket(sha256(), get()));

	public BocasJerseyTest() throws InterruptedException {
		Thread.sleep(2000);
	}

	@Override
	protected AppDescriptor configure() {
		Set<Class<?>> set = Sets.newHashSet();
		set.add(TestBocasResource.class);
		Class<?>[] classes = new Class<?>[set.size()];
		set.toArray(classes);
		return new LowLevelAppDescriptor.Builder(classes).build();
	}

	@Test
	public void test() throws Exception {
		Bocas client = BocasClientFactory.create().get(getBaseURI(), get()).getBucket("test");
		// A simple test
		byte[] data = RandomSupport.getBytes(3072);
		ByteString key = client.put(MemoryByteSource.wrap(data));
		assertTrue(SERVER.getBucket("test").contains(key));
		assertTrue(client.contains(key));
		ByteSource v = client.get(key).get();
		Assert.assertArrayEquals(data, v.read());
		client.get(ImmutableSet.of(key));
		// Now excercise
		BocasExerciser.exercise(client);
	}
	
}
