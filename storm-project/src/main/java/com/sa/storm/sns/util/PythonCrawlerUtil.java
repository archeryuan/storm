package com.sa.storm.sns.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonCrawlerUtil {

	public static final String SEARCH_URL = "https://www.facebook.com/search/str/";

	public static final String SUFFIX = "/keywords_top";

	public static final String CRAWL_USER_ID_PYTHON_SCRIPTY_PATH = "/var/lib/tomcat6/fb/fb-user.py";

	private static final Logger log = LoggerFactory.getLogger(PythonCrawlerUtil.class);

	private PythonCrawlerUtil() {

	}

	private static class Holder {
		public static final PythonCrawlerUtil INSTANCE = new PythonCrawlerUtil();
	}

	public static PythonCrawlerUtil getInstance() {
		return Holder.INSTANCE;
	}

	/**
	 * @description
	 * 
	 * @param
	 * @return
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws
	 * 
	 * @author Luke
	 * @created date 2015-3-6
	 * @modification history<BR>
	 *               No. Date Modified By <BR>
	 *               Why & What</BR> is modified
	 * 
	 * @see
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		String pythonScriptPath = args[0];
		String username = args[1];
		String password = args[2];
		String outputFile = args[3];
		String url = args[4];
		String userIds = PythonCrawlerUtil.getInstance().getUserIdbyPythonCrawler(pythonScriptPath, username, password, outputFile, url);

	}

	public List<String> getUserIds(String pythonScriptPath, String emailOrPhoneNumber) throws IOException, InterruptedException {
		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add(pythonScriptPath);

		String url = buildQueryUrl(emailOrPhoneNumber);

		cmd.add("-url");
		cmd.add(url);
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);

		Process process = pb.start();

		int exitValue = process.waitFor();
		log.info("exitValue: " + exitValue);

		int len;
		if ((len = process.getErrorStream().available()) > 0) {
			byte[] buf = new byte[len];
			process.getErrorStream().read(buf);
			log.error("Command error:\t\"" + new String(buf) + "\"");
		}

		InputStream in = process.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String result = br.readLine();

		List<String> userIdList = new ArrayList<String>();
		if (null != result && !"".equals(result)) {
			String[] userIds = result.split(",");
			for (String userId : userIds) {
				userIdList.add(userId);
			}
		}

		return userIdList;

	}

	public String getUserIdbyPythonCrawler(String pythonScriptPath, String username, String password, String outputFile, String url)
			throws IOException, InterruptedException {

		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add(pythonScriptPath);
		cmd.add("-username");
		cmd.add(username);
		cmd.add("-password");
		cmd.add(password);
		cmd.add("-file");
		cmd.add(outputFile);
		cmd.add("-url");
		cmd.add(url);
		ProcessBuilder pb = new ProcessBuilder(cmd);
		log.info("begin process {} ", cmd.toString());
		pb.redirectErrorStream(true);

		Process process = pb.start();

		int exitValue = process.waitFor();
		log.info("exitValue: " + exitValue);
		String userId = null;
		if (exitValue == 0) {
			File file = new File(outputFile);
			List<String> list = FileUtils.readLines(file, "UTF-8");
			userId = list.get(0);
			file.delete();
		}

		return userId;
	}

	public String getUserJson(String pythonScriptPath, String emailOrPhoneNumber) throws IOException, InterruptedException {
		List<String> cmd = new ArrayList<String>();
		cmd.add("python");
		cmd.add(pythonScriptPath);

		String url = buildQueryUrl(emailOrPhoneNumber);

		cmd.add("-url");
		cmd.add(url);
		log.info("begin to process");
		ProcessBuilder pb = new ProcessBuilder(cmd);
		pb.redirectErrorStream(true);

		log.info("process start");
		Process process = pb.start();

		log.info("process waitFor");
		int exitValue = process.waitFor();
		log.info("exitValue: " + exitValue);

		int len;
		if ((len = process.getErrorStream().available()) > 0) {
			byte[] buf = new byte[len];
			process.getErrorStream().read(buf);
			log.error("Command error:\t\"" + new String(buf) + "\"");
		}

		InputStream in = process.getInputStream();
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String result = br.readLine();
		log.info("result: {}", result);

		br.close();
		in.close();
		return result;

	}

	protected String buildQueryUrl(String emailOrPhoneNumber) {
		return SEARCH_URL + emailOrPhoneNumber + SUFFIX;
	}

}
