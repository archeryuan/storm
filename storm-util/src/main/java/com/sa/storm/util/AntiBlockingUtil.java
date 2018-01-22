/**
 * 
 */
package com.sa.storm.util;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.sa.redis.definition.RedisDefinition.StormSpiderDef;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.exception.EmptyUrlException;

/**
 * Anti-blocking utility<BR>
 * 
 * 
 * @author lewis
 * 
 */
public class AntiBlockingUtil {
	private static final Logger log = LoggerFactory.getLogger(AntiBlockingUtil.class);

	private static final int SLEEP_TIME_IN_SECOND = 10;
	private static final String DELIMITER = "-";
	private static String IP;

	private final String url;
	private final String key;
	private final int timeToSleep;

	private boolean inCrawling;

	static {
		try {
			IP = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			log.error("Failed getting local host IP address");
			log.error(e.getMessage(), e);
		}
	}

	/**
	 * @param url
	 * @throws EmptyUrlException
	 * @throws URISyntaxException
	 * @throws MalformedURLException
	 */
	public AntiBlockingUtil(String url) throws EmptyUrlException, URISyntaxException, MalformedURLException {
		if (StringUtils.isBlank(url)) {
			throw new EmptyUrlException("URL is empty!");
		}

		this.url = url;
		this.key = generateKey(url);
		this.timeToSleep = SLEEP_TIME_IN_SECOND;
	}

	/**
	 * 
	 * @param url
	 * @param timeToSleep
	 * @throws EmptyUrlException
	 * @throws URISyntaxException
	 * @throws MalformedURLException
	 */
	public AntiBlockingUtil(String url, int timeToSleep) throws EmptyUrlException, URISyntaxException, MalformedURLException {
		if (StringUtils.isBlank(url)) {
			throw new EmptyUrlException("URL is empty!");
		}

		this.url = url;
		this.key = generateKey(url);
		this.timeToSleep = timeToSleep;
	}

	public static void blockingTriggered(String type, String msg) {
		Jedis jedis = null;
		try {
			jedis = RedisUtil.getInstance().getResource();

			final String key = generateBlockingKey(type);

			jedis.set(key, String.valueOf(msg));

		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				RedisUtil.getInstance().returnResource(jedis);
			} catch (Exception e) {
			}
		}
	}

	/**
	 * @description
	 * Enable request search engine.
	 * @param
	 * @return
	 * @exception
	 *
	 * @author                             Luke
	 * @created date                       Aug 4, 2014
	 * @modification history<BR>
	 * No.        Date          Modified By             <BR>Why & What</BR> is modified  
	 *
	 * @see
	 */
	public void enableCrawl() {
		Jedis jedis = null;
		try {
			jedis = RedisUtil.getInstance().getResource();

			final String key = getKey();
			jedis.del(key);

		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				RedisUtil.getInstance().returnResource(jedis);
			} catch (Exception e) {
			}
		}

	}

	/**
	 * @description Check whether be allow to request in search engine.
	 * @param
	 * @return
	 * @exception
	 * 
	 * @author Luke
	 * @created date Aug 4, 2014
	 * @modification history<BR>
	 *               No. Date Modified By <BR>
	 *               Why & What</BR> is modified
	 * 
	 * @see
	 */
	public boolean canCrawl() {
		Jedis jedis = null;
		try {
			jedis = RedisUtil.getInstance().getResource();

			final String key = getKey();
			long value = jedis.incr(key);
			if (1 == value) {
				this.setInCrawling(true);
				jedis.expire(key, timeToSleep);
				return true;
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				RedisUtil.getInstance().returnResource(jedis);
			} catch (Exception e) {
			}
		}
		return false;
	}

	public boolean startCrawling() {
		Jedis jedis = null;
		try {
			jedis = RedisUtil.getInstance().getResource();

			final String key = getKey();
			long value = jedis.incr(key);
			if (1 == value) {
				this.setInCrawling(true);
				jedis.expire(key, timeToSleep);
				return true;
			} else if (value > 20) {
				// Back door to remove key/value that failed to expire in case of application/server failure
				jedis.del(key);
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {
			try {
				RedisUtil.getInstance().returnResource(jedis);
			} catch (Exception e) {
			}
		}
		return false;
	}

	public void endCrawling() {
		if (isInCrawling()) {
			this.setInCrawling(false);
		}
	}

	private String generateKey(String url) throws URISyntaxException, MalformedURLException {
		return new StringBuilder().append(StormSpiderDef.ANTI_BLOCKING_PREFIX).append(IP).append(DELIMITER).append(getDomain(url))
				.toString();
	}

	private static String generateBlockingKey(String type) throws URISyntaxException, MalformedURLException {
		return new StringBuilder().append(StormSpiderDef.BLOCKING_PREFIX).append(IP).append(DELIMITER).append(type).toString();
	}

	public static String getDomain(final String strUrl) throws URISyntaxException, MalformedURLException {
		final URL url = new URL(StringUtils.remove(strUrl, " "));
		String host = url.getHost();
		if (host != null) {
			return CustomDomain.translateDomain((host.startsWith("www.") ? host.substring(4) : host));
		} else {
			throw new URISyntaxException(strUrl, "can not get host");
		}
	}

	/**
	 * @return the url
	 */
	protected String getUrl() {
		return url;
	}

	/**
	 * @return the key
	 */
	protected String getKey() {
		return key;
	}

	/**
	 * @return the inCrawling
	 */
	public boolean isInCrawling() {
		return inCrawling;
	}

	/**
	 * @param inCrawling
	 *            the inCrawling to set
	 */
	public void setInCrawling(boolean inCrawling) {
		this.inCrawling = inCrawling;
	}

	/**
	 * Custom domain mapping for AntiBlockingUtil
	 * 
	 * @author lewis
	 * 
	 */
	public enum CustomDomain {
		/**
		 * SINA
		 */
		SINA_NEWS("news.sina.com.cn", "sina.com.cn"), SINA_FINANCE("finance.sina.com.cn", "sina.com.cn"), SINA_EDU("edu.sina.com.cn",
				"sina.com.cn"), SINA_TECH("tech.sina.com.cn", "sina.com.cn"), SINA_AUTO("auto.sina.com.cn", "sina.com.cn"), SINA_SPORTS(
				"sports.sina.com.cn", "sina.com.cn"), SINA_ENT("ent.sina.com.cn", "sina.com.cn"), SINA_COLLECTION("collection.sina.com.cn",
				"sina.com.cn"), SINA_ELADIES("eladies.sina.com.cn", "sina.com.cn"), SINA_FASHION("fashion.sina.com.cn", "sina.com.cn"), SINA_GAMES(
				"games.sina.com.cn", "sina.com.cn"), SINA_ASTRO("astro.sina.com.cn", "sina.com.cn"), SINA_HISTORY("history.sina.com.cn",
				"sina.com.cn"), SINA_BABY("baby.sina.com.cn", "sina.com.cn"), SINA_STYLE("style.sina.com.cn", "sina.com.cn"),
		/**
		 * club.china.com
		 */
		CLUB_CHINA_COM_R2("r2.club.china.com", "club.china.com"), CLUB_CHINA_COM_R("r.club.china.com", "club.china.com"), CLUB_CHINA_COM_TUKU(
				"tuku.club.china.com", "club.china.com"), CLUB_CHINA_COM_IMAGE("image.club.china.com", "club.china.com");

		/**
		 * Original domain
		 */
		private final String fromDomain;

		/**
		 * Domain that "original domain" will be mapped to
		 */
		private final String toDomain;

		/**
		 * @param fromDomain
		 * @param toDomain
		 */
		private CustomDomain(String fromDomain, String toDomain) {
			this.fromDomain = fromDomain;
			this.toDomain = toDomain;
		}

		/**
		 * @return the fromDomain
		 */
		String getFromDomain() {
			return fromDomain;
		}

		/**
		 * @return the toDomain
		 */
		String getToDomain() {
			return toDomain;
		}

		/**
		 * Translate to custom domain
		 * 
		 * @param domain
		 * @return
		 */
		static String translateDomain(String domain) {
			if (StringUtils.isBlank(domain)) {
				return domain;
			}

			for (CustomDomain customDomain : CustomDomain.values()) {
				if (customDomain.getFromDomain().equalsIgnoreCase(domain)) {
					return customDomain.getToDomain();
				}
			}

			return domain;
		}
	}
}
