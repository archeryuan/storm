/**
 * 
 */
package com.sa.storm.util;

import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.sa.redis.definition.RedisDefinition.StormDef;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.exception.InvalidInputException;

/**
 * @author lewis
 * 
 */
public class TestContentDedupUtil {
	private static final Logger log = LoggerFactory.getLogger(TestContentDedupUtil.class);

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {

		Jedis jedis = null;

		try {
			jedis = RedisUtil.getInstance().getResource();
			// jedis.srem(StormDef.URL_HEX_SET, removeValue.toArray(new String[] {}));
			jedis.del(StormDef.URL_HEX_SET);
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
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testUrls() throws UnsupportedEncodingException, URISyntaxException {
		String url1 = null;
		String url2 = null;

		List<String> reservedParam = new ArrayList<String>();
		reservedParam.add("action");
		reservedParam.add("boardId");
		reservedParam.add("pageNo");
		url1 = "http://bbs1.people.com.cn/boardList.do?action=postList&boardId=6&pageNo=2";
		log.info("URL: {}, Massage URL: {}", url1, ContentDedupUtil.massageUrl(url1, reservedParam));

		url1 = "http://www.uwants.com/archiver/?fid-641-page-3.html";
		log.info("URL: {}, Massage URL: {}", url1, ContentDedupUtil.massageUrl(url1, null));

		url1 = "https://www.google.com/url?q=http://www.mobile01.com/topicdetail.php%3Ff%3D474%26t%3D2366605&sa=U&ei=u9XwUezbF7TlygG55YCgBA&ved=0CA0QFjAD&client=internal-uds-cse&usg=AFQjCNF4mxAwLsVcY8hRkYGCu9qp9YvYBA";
		log.info(url1);
		log.info(ContentDedupUtil.massageUrl(url1, null));

	}

	@Test
	public void testWithSessionParam() throws UnsupportedEncodingException, URISyntaxException, InvalidInputException {
		String url1 = "http://www.uwants.com/forumdisplay.php?fid=259&page=2";
		String url2 = "http://www.uwants.com/forumdisplay.php?fid=259&sId=929238&page=2";
		String title = "Just a title";
		String content = "A very short content.";
		String userId = "124334 or abcde";

		List<String> reservedParam = new ArrayList<String>();
		reservedParam.add("fid");
		reservedParam.add("page");

		String result1 = DigestUtils.sha512Hex(ContentDedupUtil.getRawStr(url1, reservedParam, title, userId, content, null));
		String result2 = DigestUtils.sha512Hex(ContentDedupUtil.getRawStr(url2, reservedParam, title, userId, content, null));

		log.info("result1: {}", result1);

		log.info("result2: {}", result2);
		Assert.assertEquals(result1, result2);
	}

	@Test
	public void testWithSessionParam2() throws UnsupportedEncodingException, URISyntaxException, InvalidInputException {
		String url1 = "http://www.uwants.com/forumdisplay.php?fid=259&page=2";
		String url2 = "http://www.uwants.com/forumdisplay.php?fid=259&sId=929238&page=2";
		String title = "Just a title";
		String content = "A very short content.";
		String userId = "124334 or abcde";

		// List<String> reservedParam = new ArrayList<String>();
		// reservedParam.add("fid");
		// reservedParam.add("page");

		String result1 = DigestUtils.sha512Hex(ContentDedupUtil.getRawStr(url1, null, title, userId, content, null));
		String result2 = DigestUtils.sha512Hex(ContentDedupUtil.getRawStr(url2, null, title, userId, content, null));

		log.info("result1: {}", result1);

		log.info("result2: {}", result2);
		Assert.assertNotSame(result1, result2);
	}

	@Test
	public void testIsExists() throws Exception {
		String url1 = "http://www.uwants.com/forumdisplay.php?fid=259&page=2";
		String url2 = "http://www.uwants.com/forumdisplay.php?fid=259&sId=929238&page=2";
		String title = "Just a title";
		String content = "A very short content.";
		String userId = "124334 or abcde";

		List<String> reservedParam = new ArrayList<String>();
		reservedParam.add("fid");
		reservedParam.add("page");

		Assert.assertFalse(ContentDedupUtil.isExists(url1, reservedParam, title, userId, content, null));
		Assert.assertTrue(ContentDedupUtil.isExists(url2, reservedParam, title, userId, content, null));
	}
}
