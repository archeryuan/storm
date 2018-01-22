package com.sa.storm.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sa.common.json.JsonUtil;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.domain.persistence.TrackLog;
import com.sa.storm.util.CommonStormConfiguration;

public class TrackLogService {

	private SimpleDateFormat dateFormat;

	private RedisUtil redisUtil;

	private ObjectMapper jsonMapper;

	private int trackLogTimeout;

	public TrackLogService() throws Exception {
		this.redisUtil = RedisUtil.getInstance();
		this.jsonMapper = JsonUtil.getMapper();
		this.dateFormat = new SimpleDateFormat("yyyyMMdd");
		this.trackLogTimeout = CommonStormConfiguration.getInstance().getTrackLogTimeout();
	}

	public void saveLog(TrackLog trackLog) throws Exception {
		Jedis redis = null;
		final String trackLogKey = buildTrackLogKey();
		final String trackLogJson = jsonMapper.writeValueAsString(trackLog);

		try {
			redis = getRedisResource();

			boolean isKeyExists = redis.exists(trackLogKey);

			if (isKeyExists) {
				redis.rpushx(trackLogKey, trackLogJson);
			} else {
				// If the log bucket for today does not exists, create a bucket
				// and set the expiry
				Transaction txn = redis.multi();
				txn.rpush(trackLogKey, trackLogJson);
				txn.expire(trackLogKey, trackLogTimeout);
				txn.exec();
			}
		} finally {
			returnRedisResouce(redis);
		}
	}

	/**
	 * Get logs by date
	 * 
	 * @param targetDate
	 * @return
	 * @throws Exception
	 */
	public List<TrackLog> getLogsByDate(Date targetDate) throws Exception {
		return getLogsByDate(targetDate, 0);
	}

	/**
	 * Get logs by date with max number of record counting from latest record
	 * 
	 * @param targetDate
	 * @param maxNumRecords
	 * @return
	 * @throws Exception
	 */
	public List<TrackLog> getLogsByDate(Date targetDate, long maxNumRecords) throws Exception {
		Jedis redis = null;
		final String trackLogKey = buildTrackLogKey(targetDate);

		try {
			redis = getRedisResource();
			List<String> logsJson = redis.lrange(trackLogKey, -maxNumRecords, -1);
			List<TrackLog> trackLogs = new ArrayList<TrackLog>();

			for (String logJson : logsJson) {
				TrackLog trackLog = jsonMapper.readValue(logJson, TrackLog.class);
				trackLogs.add(trackLog);
			}

			return trackLogs;
		} finally {
			returnRedisResouce(redis);
		}
	}

	private String buildTrackLogKey() {
		return buildTrackLogKey(new Date());
	}

	private String buildTrackLogKey(Date date) {
		return RedisDefinition.StormDef.TRACK_LOG_PREFIX + dateFormat.format(date);
	}

	private Jedis getRedisResource() {
		return redisUtil.getResource();
	}

	private void returnRedisResouce(Jedis redis) {
		if (redis != null) {
			redisUtil.returnResource(redis);
		}
	}
}
