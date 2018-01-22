package com.sa.storm.service;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Maintain a map of collection in memory
 * 
 * @author Kelvin Wong
 * 
 * @param <K>
 * @param <V>
 */
public class SimpleMemoryCacheService<K, V> {

	private Map<K, Set<V>> keyMap;

	private Map<K, Long> updateTimeMap;

	public SimpleMemoryCacheService() {
		keyMap = new HashMap<K, Set<V>>();
		updateTimeMap = new HashMap<K, Long>();
	}

	public void write(K key, V value) {
		Set<V> existingValues = getValue(key);

		if (existingValues == null) {
			existingValues = new HashSet<V>();
		}

		if (!existingValues.contains(value)) {
			existingValues.add(value);
			keyMap.put(key, existingValues);
			updateTimeMap.put(key, System.currentTimeMillis());
		}
	}

	public Set<K> getKeys() {
		return keyMap.keySet();
	}

	public Long getUpdateTime(K key) {
		if (updateTimeMap != null) {
			return updateTimeMap.get(key);
		} else {
			return null;
		}
	}

	public Set<V> getValue(K key) {
		return keyMap.get(key);
	}

	public Set<V> getValues() {
		if (keyMap == null || keyMap.isEmpty()) {
			return null;
		}

		Set<V> values = new HashSet<V>();

		for (K key : keyMap.keySet()) {
			values.addAll(getValue(key));
		}

		return values;
	}

	public int size() {
		if (keyMap == null || keyMap.isEmpty()) {
			return 0;
		} else {
			int size = 0;

			for (K key : keyMap.keySet()) {
				Set<V> values = getValue(key);
				size += values.size();
			}

			return size;
		}
	}

	public void remove(K key) {
		if (keyMap != null) {
			keyMap.remove(key);
			updateTimeMap.remove(key);
		}
	}

	public void clear() {
		if (keyMap != null) {
			keyMap.clear();
			updateTimeMap.clear();
		}
	}
}
