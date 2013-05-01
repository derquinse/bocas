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

import static net.derquinse.bocas.BocasHashFunction.sha256;
import static net.derquinse.bocas.BocasServices.shared;
import static net.derquinse.common.io.MemoryByteSourceLoader.get;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.io.MemoryByteSource;

import org.testng.annotations.Test;

/**
 * Test for multiservice cache.
 */
public class MultiServiceTest {
	private static Bocas newBucket() {
		return BocasServices.memoryBucket(sha256(), get());
	}

	@Test
	public void service() throws Exception {
		BocasService primaryService = shared(newBucket());
		BocasService secondaryService = shared(newBucket());
		final Bocas b1 = primaryService.getBucket("test");
		final Bocas b2 = secondaryService.getBucket("test");
		MultiServiceGuavaCache cache = BocasServices.multiServiceCache().build();
		final Bocas primary = cache.decorate(primaryService).getBucket("test");
		final Bocas secondary = cache.decorate(secondaryService).getBucket("test");
		Map<ByteString, MemoryByteSource> set1 = BocasExerciser.dataSet(sha256(), 20);
		Map<ByteString, MemoryByteSource> set2 = BocasExerciser.dataSet(sha256(), 20);
		Map<ByteString, MemoryByteSource> set3 = BocasExerciser.dataSet(sha256(), 20);
		assertTrue(primary.get(set1.keySet()).isEmpty());
		assertTrue(primary.get(set2.keySet()).isEmpty());
		assertTrue(primary.get(set3.keySet()).isEmpty());
		assertTrue(secondary.get(set1.keySet()).isEmpty());
		assertTrue(secondary.get(set2.keySet()).isEmpty());
		assertTrue(secondary.get(set3.keySet()).isEmpty());
		assertTrue(b1.get(set1.keySet()).isEmpty());
		assertTrue(b1.get(set2.keySet()).isEmpty());
		assertTrue(b1.get(set3.keySet()).isEmpty());
		assertTrue(b2.get(set1.keySet()).isEmpty());
		assertTrue(b2.get(set2.keySet()).isEmpty());
		assertTrue(b2.get(set3.keySet()).isEmpty());
		// Set 1 written to primary
		b1.putAll(set1.values());
		assertEquals(b1.get(set1.keySet()).keySet(), set1.keySet());
		assertTrue(b2.get(set1.keySet()).isEmpty());
		assertEquals(primary.get(set1.keySet()).keySet(), set1.keySet());
		assertEquals(secondary.get(set1.keySet()).keySet(), set1.keySet());
		// Set 2 written to secondary
		b2.putAll(set2.values());
		assertEquals(b2.get(set2.keySet()).keySet(), set2.keySet());
		assertTrue(b1.get(set2.keySet()).isEmpty());
		assertEquals(secondary.get(set2.keySet()).keySet(), set2.keySet());
		assertEquals(primary.get(set2.keySet()).keySet(), set2.keySet());
		// Set 3 written through cache
		primary.putAll(set3.values());
		assertTrue(b2.get(set3.keySet()).isEmpty());
		assertEquals(b1.get(set3.keySet()).keySet(), set3.keySet());
		assertEquals(primary.get(set3.keySet()).keySet(), set3.keySet());
		assertEquals(secondary.get(set3.keySet()).keySet(), set3.keySet());
		// Excercise
		BocasExerciser.exercise(primary);
		BocasExerciser.exercise(secondary);
	}

}
