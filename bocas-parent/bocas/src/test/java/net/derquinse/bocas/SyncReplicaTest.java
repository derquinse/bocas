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

import static net.derquinse.bocas.BocasServices.memoryBucket;
import static net.derquinse.bocas.BocasServices.shared;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import net.derquinse.common.base.ByteString;

import org.testng.annotations.Test;

/**
 * Test for synchronous replicas.
 */
public class SyncReplicaTest {

	private void test(Bocas primary, Bocas replica) throws Exception {
		Map<ByteString, LoadedBocasValue> set1 = BocasExerciser.dataSet(20);
		Map<ByteString, LoadedBocasValue> set2 = BocasExerciser.dataSet(20);
		Map<ByteString, LoadedBocasValue> set3 = BocasExerciser.dataSet(20);
		assertTrue(primary.contained(set1.keySet()).isEmpty());
		assertTrue(primary.contained(set2.keySet()).isEmpty());
		assertTrue(primary.contained(set3.keySet()).isEmpty());
		assertTrue(replica.contained(set1.keySet()).isEmpty());
		assertTrue(replica.contained(set2.keySet()).isEmpty());
		assertTrue(replica.contained(set3.keySet()).isEmpty());
		Bocas replicated = BocasServices.syncReplica(primary, replica, true);
		// Set 1 written to primary
		primary.putAll(set1.values());
		assertEquals(primary.contained(set1.keySet()), set1.keySet());
		assertEquals(replicated.contained(set1.keySet()), set1.keySet());
		assertTrue(replica.contained(set1.keySet()).isEmpty());
		// Set 2 written to replica
		replica.putAll(set2.values());
		assertTrue(primary.contained(set2.keySet()).isEmpty());
		assertEquals(replica.contained(set2.keySet()), set2.keySet());
		assertTrue(replicated.contained(set2.keySet()).isEmpty());
		// Set 3 replicated
		replicated.putAll(set3.values());
		assertEquals(primary.contained(set3.keySet()), set3.keySet());
		assertEquals(replica.contained(set3.keySet()), set3.keySet());
		assertEquals(replicated.contained(set3.keySet()), set3.keySet());
		// Set 1 replicated - won't be copied because it exists on primary
		replicated.putAll(set1.values());
		assertEquals(primary.contained(set1.keySet()), set1.keySet());
		assertEquals(replicated.contained(set1.keySet()), set1.keySet());
		assertTrue(replica.contained(set1.keySet()).isEmpty());
		// Set 2 replicated
		replicated.putAll(set2.values());
		assertEquals(primary.contained(set2.keySet()), set2.keySet());
		assertEquals(replica.contained(set2.keySet()), set2.keySet());
		assertEquals(replicated.contained(set2.keySet()), set2.keySet());
	}

	@Test
	public void bucket() throws Exception {
		Bocas primary = memoryBucket();
		Bocas replica = memoryBucket();
		test(primary, replica);
	}

	@Test
	public void service() throws Exception {
		BocasService primary = shared(memoryBucket());
		BocasService replica = shared(memoryBucket());
		test(primary.getBucket("test"), replica.getBucket("test"));
	}

}
