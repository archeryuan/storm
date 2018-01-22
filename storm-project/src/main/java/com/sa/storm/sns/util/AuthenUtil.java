/**
 *Copyright(C) ©2015 Social Hunter. All rights reserved.
 *
 */
package com.sa.storm.sns.util;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sa.redis.definition.RedisDefinition.CompetitorHunterDef;
import com.sa.redis.util.RedisUtil;

/**
 * @author Luke
 *
 */
public final class AuthenUtil {
	private static final Logger log = LoggerFactory.getLogger(AuthenUtil.class);
	public static RedisUtil redisUtil;

	static {
		try {
			redisUtil = RedisUtil.getFrontendInstance();
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	private static LoadingCache<String, String> wsIdMap = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES)
			.build(new CacheLoader<String, String>() {
				@Override
				public String load(String key) {
					return StringUtils.defaultString(redisUtil.hget(CompetitorHunterDef.WEBSOCKET_MAP, key), "");
				}
			});

	/**
	 * Get web socket ID by user ID
	 * 
	 * @param userId
	 * @return
	 */
	public static String getWebsocketId(long userId) {
		String setKey = new StringBuilder().append(CompetitorHunterDef.USER_SESSION_SET_PREFIX).append(userId).toString();
		Set<String> tokens = redisUtil.smembers(setKey);

		if (null != tokens && !tokens.isEmpty()) {
			for (String token : tokens) {
				String wsId = null;
				try {
					wsId = wsIdMap.get(token);
				} catch (ExecutionException e) {
					log.error(e.getMessage(), e);
				}
				if (!StringUtils.isEmpty(wsId))
					return wsId;
			}
		}

		return null;
	}

}
