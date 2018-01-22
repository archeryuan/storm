package com.sa.storm.sns.bolt.webdriver.util;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSTest {

	private static String HDFSUrl = "hdfs://hadoop-master1:9000/user/sa/facebook/";

	public static void main(String[] args) throws IOException {

		// uploadLocalFile2HDFS("D://nike12", HDFSUrl);
		appendFile("D://nike", HDFSUrl + "/relation/182162001806727-15087023444");
		//appendContent();
	}

	public static void uploadLocalFile2HDFS(String s, String d) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path src = new Path(s);
		Path dst = new Path(d);

		hdfs.copyFromLocalFile(src, dst);

		hdfs.close();
	}

	public static void appendFile(String localFile, String hdfsPath) throws IOException {
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		InputStream in = new FileInputStream(localFile);
		OutputStream out = fs.append(new Path(hdfsPath));
		IOUtils.copyBytes(in, out, config);
	}

	public static void appendContent() {
		String hdfs_path = HDFSUrl + "nike12";
		Configuration conf = new Configuration();
		conf.setBoolean("dfs.support.append", true);

		String inpath = "D://nike";
		FileSystem fs = null;
		try {
			fs = FileSystem.get(URI.create(hdfs_path), conf);
			InputStream in = new BufferedInputStream(new FileInputStream(inpath));
			OutputStream out = fs.append(new Path(hdfs_path));
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
