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
package net.derquinse.bocas.je;

import net.derquinse.bocas.Bocas;
import net.derquinse.bocas.BocasExerciser;

import org.testng.annotations.Test;

import com.google.common.io.Files;

/**
 * Test for JEBocas.
 * @author Andres Rodriguez.
 */
public class JEBocasTest {
	public JEBocasTest() {
	}

	private static String dir() {
		return Files.createTempDir().getAbsolutePath();
	}

	private void test(Bocas bocas) throws Exception {
		try {
			BocasExerciser.exercise(bocas);
		} finally {
			bocas.close();
		}
	}

	@Test
	public void basic() throws Exception {
		test(JEBocasServices.basic(dir()));
	}

	@Test
	public void direct() throws Exception {
		test(JEBocasServices.newBuilder().direct().build(dir()));
	}

	@Test
	public void sharedCache() throws Exception {
		test(JEBocasServices.newBuilder().setFileSizeMB(32).setCacheSizeMB(2).shared().build(dir()));
	}

	@Test(expectedExceptions = Exception.class)
	public void readOnly() throws Exception {
		test(JEBocasServices.newBuilder().readOnly().setCacheSizeMB(2).shared().build(dir()));
	}

}
