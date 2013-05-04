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
package net.derquinse.bocas.gcs;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasExerciser;
import net.derquinse.bocas.BocasHashFunction;
import net.derquinse.bocas.BocasService;
import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;
import net.derquinse.common.io.MemoryByteSourceLoader;

import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;

/**
 * Test for GCSBocas.
 * @author Andres Rodriguez.
 */
public class GCSBocasTest {
	private final BocasHashFunction f = BocasHashFunction.sha256();
	@SuppressWarnings("unused")
	private final MemoryByteSourceLoader loader = MemoryByteSourceLoader.get();
	private final BocasService service;

	public GCSBocasTest() throws Exception {
		// String email = "";
		// File p12 = new File("");
		// service = GCSBocas.service(email, p12, f, loader);
		service = null;
	}

	@Test
	public void simple() throws Exception {
		if (service == null) {
			return;
		}
		Bocas bocas = service.getBucket("gcstest.bocas.derquinse.net");
		MemoryByteSource value = BocasExerciser.data();
		ByteString key = f.hash(value);
		assertFalse(bocas.contains(key));
		Optional<ByteSource> notFound = bocas.get(key);
		assertFalse(notFound.isPresent());
		System.out.printf("Size: %d\n", value.size());
		bocas.put(value);
		assertTrue(bocas.contains(key));
		Optional<ByteSource> found = bocas.get(key);
		assertTrue(found.isPresent());
		BocasExerciser.check(value, found.get());
	}

	@Test
	public void exercise() throws Exception {
		if (service == null) {
			return;
		}
		BocasExerciser.exercise(service.getBucket("gcstest.bocas.derquinse.net"), 15);
	}

}
