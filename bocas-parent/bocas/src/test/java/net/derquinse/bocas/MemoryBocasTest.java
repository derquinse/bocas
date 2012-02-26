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
package net.derquinse.bocas;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

/**
 * Test for {@link MemoryBocas}.
 */
public class MemoryBocasTest {
	private Bocas memory = BocasServices.memory();

	@Test
	public void test() throws Exception {
		BocasExerciser.exercise(memory);
	}

	@Test(dependsOnMethods = "test")
	public void cached() throws Exception {
		Bocas cache = BocasServices.cache(memory, 1000L, 10L, TimeUnit.MINUTES);
		BocasExerciser.exercise(cache);
		BocasEntry e = BocasExerciser.data();
		memory.put(e.getValue());
		assertTrue(cache.contains(e.getKey()));
	}

	@Test(dependsOnMethods = "cached")
	public void fallback() throws Exception {
		final Bocas primary = BocasServices.memory();
		final Bocas fallback = BocasServices.fallback(primary, memory);
		BocasExerciser.exercise(fallback);
		BocasEntry e = BocasExerciser.data();
		assertFalse(memory.contains(e.getKey()));
		assertFalse(primary.contains(e.getKey()));
		assertFalse(fallback.contains(e.getKey()));
		memory.put(e.getValue());
		assertTrue(memory.contains(e.getKey()));
		assertFalse(primary.contains(e.getKey()));
		assertTrue(fallback.contains(e.getKey()));
	}

}
