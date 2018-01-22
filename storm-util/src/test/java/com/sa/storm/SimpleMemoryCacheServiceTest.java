package com.sa.storm;

import junit.framework.Assert;

import org.junit.Test;

import com.sa.storm.service.SimpleMemoryCacheService;

public class SimpleMemoryCacheServiceTest {

	@Test
	public void testAddValues() {
		SimpleMemoryCacheService<String, Integer> cache = new SimpleMemoryCacheService<String, Integer>();

		cache.write("A", 1);
		cache.write("A", 2);
		Assert.assertEquals("Cache size should match after adding initial value in A", 2, cache.size());

		cache.write("A", 2);
		Assert.assertEquals("Cache size should match after adding duplicate value in A", 2, cache.size());

		cache.write("A", 4);
		cache.write("A", 5);
		cache.write("A", 6);
		cache.write("A", 7);
		cache.write("A", 8);
		Assert.assertEquals("Cache size should match after adding collection in A", 7, cache.size());

		cache.write("B", 99);
		cache.write("B", 98);
		cache.write("B", 97);
		cache.write("B", 96);
		Assert.assertEquals("B cache size should match after adding collection in B", 4, cache.getValue("B").size());
		Assert.assertEquals("A cache size should remain the same after adding another key B", 7, cache.getValue("A").size());
		Assert.assertEquals("Total cache size should match after adding another collection", 11, cache.size());

		cache.clear();
	}

	@Test
	public void testRemoveValues() {
		SimpleMemoryCacheService<String, Integer> cache = new SimpleMemoryCacheService<String, Integer>();
		cache.write("A", 1);
		cache.write("A", 2);
		cache.write("A", 3);
		cache.write("A", 4);
		cache.write("B", 99);
		cache.write("B", 98);
		cache.write("B", 97);
		Assert.assertEquals("Cache size should match after adding collections", 7, cache.size());

		cache.remove("B");
		Assert.assertEquals("Cache size should match after removing key B", 4, cache.size());

		cache.getKeys();
		Assert.assertFalse("Cache should not contain B anymore", cache.getKeys().contains("B"));
		Assert.assertTrue("Cache should still contain A", cache.getKeys().contains("A"));
		Assert.assertEquals("A cache size should remain unchanged", 4, cache.getValue("A").size());
		Assert.assertTrue("A cache key should not be modified", cache.getValue("A").contains(1) && cache.getValue("A").contains(2)
				&& cache.getValue("A").contains(3) && cache.getValue("A").contains(4));
	}

	public void testClear() {
		SimpleMemoryCacheService<String, Integer> cache = new SimpleMemoryCacheService<String, Integer>();
		cache.write("A", 1);
		cache.write("A", 2);
		cache.write("A", 3);
		cache.write("A", 4);
		cache.write("B", 99);
		cache.write("B", 98);
		cache.write("B", 97);

		cache.clear();
		Assert.assertEquals("All keys should have been removed after clear", null, cache.getValues());
		Assert.assertEquals("All keys should have been removed after clear", null, cache.getValue("A"));
		Assert.assertEquals("All keys should have been removed after clear", null, cache.getValue("B"));
		Assert.assertEquals("All keys should have been removed after clear", null, cache.getKeys());
	}

}
