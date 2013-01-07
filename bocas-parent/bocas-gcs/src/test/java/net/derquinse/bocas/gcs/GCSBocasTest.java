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
import net.derquinse.bocas.BocasService;
import net.derquinse.bocas.BocasValue;
import net.derquinse.bocas.LoadedBocasValue;

import org.testng.annotations.Test;

import com.google.common.base.Optional;

/**
 * Test for GCSBocas.
 * @author Andres Rodriguez.
 */
public class GCSBocasTest {
	private final BocasService service;

	public GCSBocasTest() throws Exception {
		// String email = "";
		// File p12 = new File("");
		// service = GCSBocas.service(email, p12);
		service = null;
	}

	@Test
	public void simple() throws Exception {
		if (service == null) {
			return;
		}
		Bocas bocas = service.getBucket("gcstest.bocas.derquinse.net");
		LoadedBocasValue value = BocasExerciser.data();
		assertFalse(bocas.contains(value.key()));
		Optional<BocasValue> notFound = bocas.get(value.key());
		assertFalse(notFound.isPresent());
		System.out.printf("Size: %d\n", value.getSize());
		bocas.put(value);
		assertTrue(bocas.contains(value.key()));
		Optional<BocasValue> found = bocas.get(value.key());
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
