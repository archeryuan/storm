/**
 * Content de-duplication utility
 */
package com.sa.storm.util;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.sa.redis.definition.RedisDefinition.StormDef;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.exception.InvalidInputException;

/**
 * Content de-duplication utility
 * 
 * @author lewis
 * 
 */
public class ContentDedupUtil {
	private static final Logger log = LoggerFactory.getLogger(ContentDedupUtil.class);

	public static void main(String args[]) throws ParseException {
		String s = "22/1/2013 6:21";
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
		log.info("{}", sdf.parse(s));
	}

	public static boolean isExists(String url, List<String> urlReservedParams, String title, String uId, String content, Long publishDate)
			throws Exception {

		String rawStr = getRawStr(url, urlReservedParams, title, uId, content, publishDate);
		String hex = DigestUtils.sha512Hex(rawStr);

		Jedis jedis = null;

		try {
			jedis = RedisUtil.getInstance().getResource();
			long added = jedis.sadd(StormDef.URL_HEX_SET, hex);

			if (added == 1) {
				return false;
			} else {
				log.info("Content already exists. URL({}), Title({}), UserID({}), Content({}), PublishDate({})", new Object[] { url, title,
						uId, content, publishDate });
				return true;
			}
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				RedisUtil.getInstance().returnResource(jedis);
			} catch (Exception e) {
			}
		}
	}

	/**
	 * Validate input parameters and return raw content string<BR>
	 * Raw string contains: URL+Title+UserID+Content+pDate
	 * 
	 * @param inUrl
	 * @param urlReservedParams
	 * @param inTitle
	 * @param inUserId
	 * @param inContent
	 * @param publishDate
	 * @return
	 * @throws URISyntaxException
	 * @throws InvalidInputException
	 * @throws UnsupportedEncodingException
	 */
	protected static String getRawStr(String inUrl, List<String> urlReservedParams, String inTitle, String inUserId, String inContent,
										Long publishDate) throws URISyntaxException, InvalidInputException, UnsupportedEncodingException {
		if (StringUtils.isBlank(inUrl)) {
			throw new InvalidInputException("URL is empty.");
		}

		final StringBuilder sb = new StringBuilder();
		sb.append(massageUrl(inUrl, urlReservedParams));

		if (!StringUtils.isBlank(inTitle)) {
			sb.append(inTitle);
		}

		if (!StringUtils.isBlank(inUserId)) {
			sb.append(inUserId);
		}

		if (!StringUtils.isBlank(inContent)) {
			sb.append(inContent);
		}

		if (publishDate != null) {
			sb.append(publishDate);
		}

		String rawStr = sb.toString();
		log.debug("rawStr: {}", rawStr);
		return rawStr;
	}

	/**
	 * Massage URL<BR>
	 * 1) drop fragment<BR>
	 * 2) if urlReservedParams is not null, drop url parameters which is not in urlReservedParams
	 * 
	 * @param url
	 * @param urlReservedParams
	 * @return
	 * @throws URISyntaxException
	 * @throws UnsupportedEncodingException
	 */
	public static String massageUrl(String url, List<String> urlReservedParams) throws URISyntaxException, UnsupportedEncodingException {
		if (StringUtils.isBlank(url)) {
			return null;
		}

		URI uri = new URI(url);
		String query = null;

		if (urlReservedParams != null && urlReservedParams.size() > 0 && !StringUtils.isBlank(uri.getRawQuery())) {
			Map<String, String> paramMap = new HashMap<String, String>();

			String[] params = StringUtils.split(uri.getQuery(), "&");
			for (String param : params) {
				String[] tmp = StringUtils.split(param, "=");
				if (tmp.length == 2 && !StringUtils.isBlank(tmp[0])) {
					paramMap.put(tmp[0].trim().toLowerCase(Locale.ENGLISH), tmp[1]);
				}
			}

			Map<String, String> resultMap = new HashMap<String, String>();
			for (String reservedParam : urlReservedParams) {
				if (!StringUtils.isBlank(reservedParam)) {
					String tmp = reservedParam.trim().toLowerCase(Locale.ENGLISH);
					if (paramMap.containsKey(tmp)) {
						resultMap.put(tmp, paramMap.get(tmp));
					}
				}
			}

			StringBuilder querySb = new StringBuilder();
			for (int i = 0; i < urlReservedParams.size(); ++i) {
				String key = urlReservedParams.get(i);
				if (resultMap.containsKey(key)) {
					if (querySb.length() > 0) {
						querySb.append("&");
					}
					querySb.append(key).append("=").append(resultMap.get(key));
				}
			}

			if (querySb.length() > 0)
				query = querySb.toString();
		} else {
			query = uri.getQuery();
		}

		uri = new URI(uri.getScheme(), uri.getRawAuthority(), uri.getRawPath(), null, null);

		return uri.toString() + (query == null ? "" : "?" + query);
	}

	// public static void main(String[] args) throws URISyntaxException, UnsupportedEncodingException, InvalidInputException {
	// String url =
	// "http://www.mobile01.com/topicdetail.php%3Ff%3D292%26t%3D1313790%26p%3D1436&sa=U&ei=It3wUYSEMsecyQHxlICoCA&ved=0CA8QFjAEOB4&client=internal-uds-cse&usg=AFQjCNHR18GAknjclIx9fMhbadfEGbZykw";
	// log.info(URLDecoder.decode(url, "utf-8"));
	// }
}
