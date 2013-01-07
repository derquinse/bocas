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

import org.testng.annotations.Test;

/**
 * Test for {@link DirectMemoryBocas}.
 */
public class DirectMemoryBocasTest {
	private BocasService memory = BocasServices.shared(BocasServices.directBucket());

	@Test
	public void test() throws Exception {
		BocasExerciser.exercise(memory.getBucket("test"));
	}

	@Test(dependsOnMethods = "test")
	public void cached() throws Exception {
		BocasExerciser.cached(memory, "test");
	}

	@Test(dependsOnMethods = "cached")
	public void fallback() throws Exception {
		BocasExerciser.fallback(memory, "test");
	}

}
