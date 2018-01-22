package com.sa.storm.util;

import java.util.HashMap;

public class MemoryCacheManager {

	private static HashMap<String, String> cacheData;

	static {
		cacheData = new HashMap<String, String>();
	}

	public static void save(String key, String data) {
		cacheData.put(key, data);
	}

	public static String load(String key) {
		return cacheData.get(key);
	}
}
