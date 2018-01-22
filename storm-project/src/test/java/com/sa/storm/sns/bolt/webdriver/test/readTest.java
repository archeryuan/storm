package com.sa.storm.sns.bolt.webdriver.test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class readTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		File file = new File("D://nike1");
		List<String> list = FileUtils.readLines(file, "UTF-8");
		String detailInfo = list.get(0);
		String friendNum = detailInfo.split("\\|")[0];
		String liveIn = detailInfo.split("\\|")[1];
		System.out.println(friendNum+ "  "+ liveIn);
	}

}
