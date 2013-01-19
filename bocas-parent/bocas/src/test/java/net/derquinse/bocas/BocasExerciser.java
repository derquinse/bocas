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
import java.util.concurrent.atomic.AtomicLong;

import net.derquinse.common.base.ByteString;
import net.derquinse.common.test.RandomSupport;
import net.derquinse.common.util.zip.MaybeCompressed;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Longs;

/**
 * Bocas bucket exerciser.
 */
public final class BocasExerciser {
	/** New data index. */
	private static final AtomicLong INDEX = new AtomicLong(0L);

	/** Repository. */
	private final Bocas bocas;
	/** Error flag. */
	private boolean ok = true;
	/** Number of tasks. */
	private final int tasks;

	public static LoadedBocasValue data() {
		byte[] deterministic = Longs.toByteArray(INDEX.incrementAndGet());
		byte[] data = RandomSupport.getBytes(RandomSupport.nextInt(1024, 10240));
		for (int i = 0; i < deterministic.length; i++) {
			data[i] = deterministic[i];
		}
		return BocasValue.of(data);
	}

	public static void check(LoadedBocasValue value, InputSupplier<? extends InputStream> data) throws IOException {
		assertEquals(ByteStreams.toByteArray(data), ByteStreams.toByteArray(value));
	}

	/** Exercises a Bocas repository. */
	public static void exercise(Bocas bocas, int tasks) throws Exception {
		BocasExerciser e = new BocasExerciser(bocas, tasks);
		e.run();
	}

	/** Exercises a Bocas repository. */
	public static void exercise(Bocas bocas) throws Exception {
		exercise(bocas, 1000);
	}

	/** Exercise a Bocas repository putting a cache in front of it. */
	public static void cached(BocasService service, String bucketName) throws Exception {
		final Bocas bucket = service.getBucket(bucketName);
		// Heap cache
		BocasService cache = BocasServices.cache().expireAfterAccess(10L, TimeUnit.MINUTES).maximumSize(1000)
				.build(service);
		Bocas cached = cache.getBucket(bucketName);
		exercise(cached);
		LoadedBocasValue value = data();
		bucket.put(value);
		assertTrue(cached.contains(value.key()));
		// Direct cache
		cache = BocasServices.cache().expireAfterAccess(10L, TimeUnit.MINUTES).maximumWeight(10000000L).direct()
				.build(service);
		cached = cache.getBucket(bucketName);
		exercise(cached);
		value = data();
		bucket.put(value);
		assertTrue(cached.contains(value.key()));
	}

	/** Exercise a Bocas repository putting it as the seed of a memory one. */
	public static void fallback(BocasService service, String bucketName) throws Exception {
		final Bocas bocas = service.getBucket(bucketName);
		final Bocas primary = BocasServices.memoryBucket();
		final Bocas fallback = BocasServices.seeded(primary, bocas);
		BocasExerciser.exercise(fallback);
		LoadedBocasValue value;
		ByteString key;
		do {
			value = data();
			key = value.key();
		} while (bocas.contains(key));
		assertFalse(primary.contains(key));
		assertFalse(fallback.contains(key));
		bocas.put(value);
		assertTrue(bocas.contains(key));
		assertFalse(primary.contains(key));
		assertTrue(fallback.contains(key));
	}

	private BocasExerciser(Bocas bocas, int tasks) {
		this.bocas = checkNotNull(bocas, "The repository to exercise must be provided");
		this.tasks = Math.max(10, tasks);
	}

	private void checkInRepository(LoadedBocasValue value) throws IOException {
		ByteString k = value.key();
		assertTrue(bocas.contains(k));
		assertTrue(bocas.contained(ImmutableSet.of(k)).contains(k));
		Optional<BocasValue> optional = bocas.get(k);
		assertTrue(optional.isPresent());
		check(value, optional.get());
		Map<ByteString, BocasValue> map = bocas.get(ImmutableSet.of(k));
		assertTrue(map.containsKey(k));
		// Check with repeatable read
		for (int i = 0; i < 5; i++) {
			check(value, map.get(k));
		}
	}

	private void put(LoadedBocasValue value) throws IOException {
		ByteString returned = bocas.put(value);
		ByteString k = value.key();
		assertEquals(returned, k);
		checkInRepository(value);
	}

	private void checkNotInRepository(LoadedBocasValue value) throws Exception {
		ByteString k = value.key();
		assertFalse(bocas.contains(k));
		assertTrue(bocas.contained(ImmutableSet.of(k)).isEmpty());
		assertFalse(bocas.get(k).isPresent());
		assertTrue(bocas.get(ImmutableSet.of(k)).isEmpty());
	}

	/** Exercise the repository. */
	private void run() throws Exception {
		LoadedBocasValue data1 = data();
		ByteString k1 = data1.key();
		put(data1);
		LoadedBocasValue data2 = data();
		ByteString k2 = data2.key();
		LoadedBocasValue data3 = data();
		ByteString k3 = data3.key();
		put(data2);
		checkInRepository(data1);
		checkNotInRepository(data3);
		LoadedBocasValue data4 = data();
		ByteString k4 = data4.key();
		List<ByteString> list = ImmutableList.of(k1, k2, k3, k4, k1);
		assertEquals(bocas.contained(list).size(), 2);
		assertEquals(bocas.get(list).size(), 2);
		List<ByteString> keys = bocas.putAll(ImmutableList.of(data2, data3));
		assertEquals(keys.size(), 2);
		assertEquals(keys.get(0), k2);
		assertEquals(keys.get(1), k3);
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
			LoadedBocasValue e = data();
			keyList.add(e.key());
			valueList.add(e);
		}
		bocas.putAll(valueList);
		assertTrue(bocas.contained(keyList).containsAll(keyList));
		assertTrue(bocas.get(keyList).keySet().containsAll(keyList));
		// Multiple operation - Phase II
		for (int i = 0; i < 15; i++) {
			LoadedBocasValue e = data();
			keyList.add(e.key());
			valueList.add(e);
		}
		bocas.putAll(valueList);
		assertTrue(bocas.contained(keyList).containsAll(keyList));
		assertTrue(bocas.get(keyList).keySet().containsAll(keyList));
		// ZIP
		ZipBocas zb = ZipBocas.of(bocas);
		Map<String, ByteString> entries = zb.putZip(getClass().getResourceAsStream("loren.zip"));
		assertEquals(bocas.contained(entries.values()).size(), 3);
		// ZIP
		Map<String, MaybeCompressed<ByteString>> mentries = zb.putZipAndGZip(getClass().getResourceAsStream("loren.zip"));
		assertEquals(bocas.contained(entries.values()).size(), 3);
		for (MaybeCompressed<ByteString> mk : mentries.values()) {
			assertTrue(mk.isCompressed());
		}
		// Concurrent operation.
		ExecutorService s = Executors.newFixedThreadPool(5);
		for (int i = 0; i < tasks; i++) {
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
				put(data());
			} catch (Exception e) {
				ok = false;
				throw new RuntimeException(e);
			}
		}
	}

}
