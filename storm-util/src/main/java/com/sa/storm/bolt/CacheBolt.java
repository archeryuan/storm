package com.sa.storm.bolt;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.storm.domain.tuple.BaseTuple;
import com.sa.storm.service.SimpleMemoryCacheService;

/**
 * A bolt that allow caching of input tuple, and process them at once when the number of tuple reach the threshold. It also support auto
 * processing of the cache if the bolt is idle for certain time, hence to safeguard the cached tuples from timeout.
 * 
 * @author Kelvin Wong
 * 
 * @param <CacheTupleType>
 * @param <CacheValueType>
 */
public abstract class CacheBolt<CacheTupleType extends BaseTuple, CacheValueType> extends BaseBolt {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(CacheBolt.class);

	private SimpleMemoryCacheService<Tuple, CacheValueType> cacheService;

	private final int cacheMaxSize;

	private final long forceProcessCacheDuration;

	private boolean isAutoAckCache;

	private boolean isAutoFailCache;

	private long lastProcessCacheTimestamp;

	public CacheBolt(int cacheMaxSize) {
		this(cacheMaxSize, 0, true, true);
	}

	public CacheBolt(int cacheMaxSize, long forceProcessCacheDuration) {
		this(cacheMaxSize, forceProcessCacheDuration, true, true);
	}

	/**
	 * 
	 * @param cacheMaxSize
	 *            Maximum number of cached tuple
	 * @param forceProcessCacheDuration
	 *            If the bolt is idle more than this duration, then it will automatically process the cache
	 * @param isAutoAckCache
	 *            When the cache is processed, does the bolt automatically ack all the cache
	 * @param isAutoFailCache
	 *            When the result of processing the cache is fail, does the bolt automatically fail all the cache
	 */
	public CacheBolt(int cacheMaxSize, long forceProcessCacheDuration, boolean isAutoAckCache, boolean isAutoFailCache) {
		super(false, false);
		this.cacheMaxSize = cacheMaxSize;
		this.forceProcessCacheDuration = forceProcessCacheDuration;
		this.isAutoAckCache = isAutoAckCache;
		this.isAutoFailCache = isAutoFailCache;
		this.lastProcessCacheTimestamp = 0l;
	}

	@Override
	abstract public void declareOutputs(OutputFieldsDeclarer declarer);

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		cacheService = new SimpleMemoryCacheService<Tuple, CacheValueType>();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> config = super.getComponentConfiguration();

		if (forceProcessCacheDuration > 0l) {
			if (config == null) {
				config = new HashMap<String, Object>();
			}
			config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, forceProcessCacheDuration / 1000l);
			log.info("Configured ticking tuple for this bolt, frequency at {}ms", forceProcessCacheDuration);
		}

		return config;
	}

	@Override
	public void process(Tuple input) throws Exception {
		boolean isProcessCache = false;

		if (isTickTuple(input)) {
			isProcessCache = isProcessTick(input);
			if (isProcessCache) {
				log.info("Processing tick tuple now, last cache process time is {}", new Date(lastProcessCacheTimestamp));
			} else {
				log.info("Do not process tick tuple due to short timeout, last cache process time is {}", new Date(
						lastProcessCacheTimestamp));
			}
			getCollector().ack(input);
		} else {
			cache(input);
			isProcessCache = getCacheSize() >= cacheMaxSize;
			if (isProcessCache) {
				log.info("Processing cache due to cache full");
			}
		}

		if (isProcessCache) {
			processCache(getCachedInputs(), getCachedValues());

			if (isAutoAckCache()) {
				ackCache();
				cacheService.clear();
				log.info("Finished processing cache, acked and clear the cache automatically");
			} else {
				log.info("Finished processing cache, does not automatically acked and clear cache");
			}

			lastProcessCacheTimestamp = System.currentTimeMillis();
			log.info("Marked last cache process time as {}", lastProcessCacheTimestamp);
		}
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		try {
			onProcessCacheError(getCachedInputs(), e);
			log.error("Error in processing cache bolt and handled error, now ack the cache automatically", e);
			if (isAutoAckCache()) {
				ackCache();
			}
		} catch (Exception e1) {
			if (isAutoFailCache()) {
				log.error("Error in handling error for cache bolt, now fail the cache", e);
				failCache();
			}
		} finally {
			if (isAutoAckCache()) {
				cacheService.clear();
				log.info("Clear cache after handling error");
			}
		}
	}

	/**
	 * Enforce no auto ack for cache
	 */
	@Override
	protected boolean isAutoAck() {
		return false;
	}

	/**
	 * Enforce no auto fail for cache
	 */
	@Override
	protected boolean isAutoFail() {
		return false;
	}

	protected boolean isAutoAckCache() {
		return isAutoAckCache;
	}

	protected boolean isAutoFailCache() {
		return isAutoFailCache;
	}

	/**
	 * Write the input into cache
	 * 
	 * @param input
	 * @throws Exception
	 */
	abstract protected void cache(Tuple input) throws Exception;

	/**
	 * When cache is full, process the cache all together
	 * 
	 * @param cachedInputs
	 * @param cachedValues
	 * @throws Exception
	 */
	abstract protected void processCache(Set<Tuple> cachedInputs, Collection<CacheValueType> cachedValues) throws Exception;

	/**
	 * Called when there are Exception thrown in processCache
	 * 
	 * @param cachedInputs
	 * @param e
	 * @throws Exception
	 */
	abstract protected void onProcessCacheError(Set<Tuple> cachedInputs, Exception e) throws Exception;

	/**
	 * Add new tuple into the bolt cache
	 * 
	 * @param input
	 * @param value
	 */
	protected void writeCache(Tuple input, CacheValueType value) {
		cacheService.write(input, value);
	}

	/**
	 * Remove a tuple from cache
	 * 
	 * @param input
	 */
	protected void removeCache(Tuple input) {
		cacheService.remove(input);
	}

	/**
	 * Ack all cache in the bolt
	 */
	protected void ackCache() {
		Set<Tuple> cachedInputs = cacheService.getKeys();
		if (cachedInputs == null) {
			return;
		}

		for (Tuple input : cachedInputs) {
			getCollector().ack(input);
		}
	}

	/**
	 * Fail all cache in the bolt
	 */
	protected void failCache() {
		Set<Tuple> cachedInputs = cacheService.getKeys();
		if (cachedInputs == null) {
			return;
		}

		for (Tuple input : cachedInputs) {
			getCollector().fail(input);
		}
	}

	/**
	 * Get all cache tuple
	 * 
	 * @return
	 */
	protected Set<Tuple> getCachedInputs() {
		return cacheService.getKeys();
	}

	/**
	 * Get all cache values
	 * 
	 * @return
	 */
	protected Collection<CacheValueType> getCachedValues() {
		return cacheService.getValues();
	}

	/**
	 * Obtain the cache value size
	 * 
	 * @return
	 */
	protected int getCacheSize() {
		return cacheService.size();
	}

	/**
	 * Check if the cache need to be forcefully processed
	 * 
	 * @param input
	 * @return
	 */
	protected boolean isProcessTick(Tuple input) {
		if (forceProcessCacheDuration > 0l) {
			return isTickTuple(input) && (System.currentTimeMillis() - lastProcessCacheTimestamp > forceProcessCacheDuration);
		} else {
			return false;
		}
	}
}
