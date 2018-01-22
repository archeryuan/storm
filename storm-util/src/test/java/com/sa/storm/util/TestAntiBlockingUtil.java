package com.sa.storm.util;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAntiBlockingUtil {
	private static final Logger log = LoggerFactory.getLogger(TestAntiBlockingUtil.class);

	@Test
	public void test() throws URISyntaxException, UnsupportedEncodingException, MalformedURLException {
		// String url = "http://sou.autohome.com.cn/?entry=43&q=" + URLEncoder.encode("%u8001%u677f", "UTF-8") + "&page=9&pvareaid=100503";
		String url = "http://sou.autohome.com.cn/?entry=43&q=%u8001%u677f&page=9&pvareaid=100503";

		log.info("URL: {}", url);
		log.info(AntiBlockingUtil.getDomain(url));
	}
}
