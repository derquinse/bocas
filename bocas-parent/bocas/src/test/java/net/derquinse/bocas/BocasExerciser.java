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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.test.RandomSupport;
import net.derquinse.common.util.zip.MaybeCompressed;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;

/**
 * Bocas repository exerciser.
 */
public final class BocasExerciser {
	/** Repository. */
	private final Bocas bocas;
	/** Error flag. */
	private boolean ok = true;

	static BocasEntry data() {
		byte[] data = RandomSupport.getBytes(RandomSupport.nextInt(1024, 10240));
		return BocasEntry.of(ByteStreams.newInputStreamSupplier(data));
	}

	private static void check(BocasEntry entry, InputSupplier<? extends InputStream> data) throws IOException {
		assertEquals(ByteStreams.toByteArray(data), ByteStreams.toByteArray(entry.getValue()));
	}

	/** Exercise a Bocas repository. */
	public static void exercise(Bocas bocas) throws Exception {
		BocasExerciser e = new BocasExerciser(bocas);
		e.run();
	}

	/** Exercise a Bocas repository putting a cache in front of it. */
	public static void cached(Bocas bocas) throws Exception {
		// Heap cache
		Bocas cache = BocasServices.cache().expireAfterAccess(10L, TimeUnit.MINUTES).maximumSize(1000).build(bocas);
		exercise(cache);
		BocasEntry e = data();
		bocas.put(e.getValue());
		assertTrue(cache.contains(e.getKey()));
		// Direct cache
		cache = BocasServices.cache().expireAfterAccess(10L, TimeUnit.MINUTES).maximumWeight(10000000L).buildDirect(bocas);
		exercise(cache);
		e = data();
		bocas.put(e.getValue());
		assertTrue(cache.contains(e.getKey()));
	}

	/** Exercise a Bocas repository putting it as the seed of a memory one. */
	public static void fallback(Bocas bocas) throws Exception {
		final Bocas primary = BocasServices.memory();
		final Bocas fallback = BocasServices.seeded(primary, bocas);
		BocasExerciser.exercise(fallback);
		BocasEntry e;
		do {
			e = data();
		} while (bocas.contains(e.getKey()));
		assertFalse(primary.contains(e.getKey()));
		assertFalse(fallback.contains(e.getKey()));
		bocas.put(e.getValue());
		assertTrue(bocas.contains(e.getKey()));
		assertFalse(primary.contains(e.getKey()));
		assertTrue(fallback.contains(e.getKey()));
	}

	private BocasExerciser(Bocas bocas) {
		this.bocas = checkNotNull(bocas, "The repository to exercise must be provided");
	}

	private void checkInRepository(BocasEntry entry) throws IOException {
		ByteString k = entry.getKey();
		assertTrue(bocas.contains(k));
		assertTrue(bocas.contained(ImmutableSet.of(k)).contains(k));
		Optional<BocasValue> optional = bocas.get(k);
		assertTrue(optional.isPresent());
		check(entry, optional.get());
		Map<ByteString, BocasValue> map = bocas.get(ImmutableSet.of(k));
		assertTrue(map.containsKey(k));
		// Check with repeatable read
		for (int i = 0; i < 5; i++) {
			check(entry, map.get(k));
		}
	}

	private void put(BocasEntry entry) throws IOException {
		ByteString returned = bocas.put(entry.getValue());
		ByteString k = entry.getKey();
		assertEquals(returned, k);
		checkInRepository(entry);
	}

	private void checkNotInRepository(BocasEntry entry) throws Exception {
		ByteString k = entry.getKey();
		assertFalse(bocas.contains(k));
		assertTrue(bocas.contained(ImmutableSet.of(k)).isEmpty());
		assertFalse(bocas.get(k).isPresent());
		assertTrue(bocas.get(ImmutableSet.of(k)).isEmpty());
	}

	private BocasEntry create() throws Exception {
		BocasEntry entry = data();
		checkNotInRepository(entry);
		return entry;
	}

	/** Exercise the repository. */
	private void run() throws Exception {
		BocasEntry data1 = create();
		put(data1);
		BocasEntry data2 = create();
		BocasEntry data3 = create();
		put(data2);
		checkInRepository(data1);
		checkNotInRepository(data3);
		BocasEntry data4 = create();
		List<ByteString> list = ImmutableList.of(data1.getKey(), data2.getKey(), data3.getKey(), data4.getKey(),
				data1.getKey());
		assertEquals(bocas.contained(list).size(), 2);
		assertEquals(bocas.get(list).size(), 2);
		List<ByteString> keys = bocas.putSuppliers(ImmutableList.of(data2.getValue(), data3.getValue()));
		assertEquals(keys.size(), 2);
		assertEquals(keys.get(0), data2.getKey());
		assertEquals(keys.get(1), data3.getKey());
		checkInRepository(data1);
		checkInRepository(data2);
		checkInRepository(data3);
		checkNotInRepository(data4);
		assertEquals(bocas.contained(list).size(), 3);
		assertEquals(bocas.get(list).size(), 3);
		// Multiple operation - Phase I
		List<ByteString> keyList = Lists.newLinkedList();
		List<BocasValue> valueList = Lists.newLinkedList();
		for (int i = 0; i < 15; i++) {
			BocasEntry e = create();
			keyList.add(e.getKey());
			valueList.add(e.getValue());
		}
		bocas.putSuppliers(valueList);
		assertTrue(bocas.contained(keyList).containsAll(keyList));
		assertTrue(bocas.get(keyList).keySet().containsAll(keyList));
		// Multiple operation - Phase II
		for (int i = 0; i < 15; i++) {
			BocasEntry e = create();
			keyList.add(e.getKey());
			valueList.add(e.getValue());
		}
		bocas.putSuppliers(valueList);
		assertTrue(bocas.contained(keyList).containsAll(keyList));
		assertTrue(bocas.get(keyList).keySet().containsAll(keyList));
		// ZIP
		Map<String, ByteString> entries = bocas.putZip(getClass().getResourceAsStream("loren.zip"));
		assertEquals(bocas.contained(entries.values()).size(), 3);
		// ZIP
		Map<String, MaybeCompressed<ByteString>> mentries = bocas
				.putZipAndGZip(getClass().getResourceAsStream("loren.zip"));
		assertEquals(bocas.contained(entries.values()).size(), 3);
		for (MaybeCompressed<ByteString> mk : mentries.values()) {
			assertTrue(mk.isCompressed());
		}
		// Concurrent operation.
		ExecutorService s = Executors.newFixedThreadPool(5);
		for (int i = 0; i < 1000; i++) {
			s.submit(new Task());
		}
		s.shutdown();
		while (!s.awaitTermination(5, TimeUnit.SECONDS))
			;
		assertTrue(ok);
	}

	private class Task implements Runnable {
		@Override
		public void run() {
			try {
				put(create());
			} catch (Exception e) {
				ok = false;
				throw new RuntimeException(e);
			}
		}
	}

}
