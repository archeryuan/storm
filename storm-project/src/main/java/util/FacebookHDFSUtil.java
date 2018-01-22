package util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.competitorhunter.domain.RelationFansInfo;

public class FacebookHDFSUtil extends Configured {

	private static final Logger log = LoggerFactory.getLogger(FacebookHDFSUtil.class);

	public FacebookHDFSUtil() {
		Configuration conf = new Configuration();
		this.setConf(conf);
	}

	public static void uploadLocalFile2HDFS(String s, String d) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path src = new Path(s);
		Path dst = new Path(d);

		hdfs.copyFromLocalFile(src, dst);

		hdfs.close();
	}
	// if file is existing, it will append 
	public static void moveLocalFile2HDFS(String s, String d) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		boolean isFileExist = hdfs.exists(new Path(d));
		if (!isFileExist) {
			Path src = new Path(s);
			Path dst = new Path(d);
			hdfs.copyFromLocalFile(src, dst);
		} else {
			InputStream in = new FileInputStream(s);
			OutputStream out = hdfs.append(new Path(d));
			IOUtils.copyBytes(in, out, config);
		}

		hdfs.close();
	}

	public static void moveLocal2HDFS(String s, String path) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		boolean isFileExist = hdfs.exists(new Path(path));

		if (!isFileExist) {
			hdfs.mkdirs(new Path(path));
		}
		Path src = new Path(s);
		Path dst = new Path(path);
		hdfs.moveFromLocalFile(src, dst);

		hdfs.close();
	}

	public static void moveHDFSFile2Local(String HDFSFile, String Local) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		boolean isFileExist = hdfs.exists(new Path(HDFSFile));
		if (isFileExist) {
			System.out.println("here");
			Path src = new Path(HDFSFile);
			Path dst = new Path(Local);
			hdfs.copyToLocalFile(src, dst);
		}
		hdfs.close();
	}

	public static void saveLocalFile2HDFS(String s, String d) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);
		boolean isFileExist = hdfs.exists(new Path(d));
		if (!isFileExist) {
			Path src = new Path(s);
			Path dst = new Path(d);
			hdfs.copyFromLocalFile(src, dst);
		}
		hdfs.close();
	}

	public static boolean isExistFile(String dest) {
		Configuration conf = new Configuration();
		boolean isFileExist = false;
		try {
			// FileSystem fs = FileSystem.get(URI.create(dest), conf);
			FileSystem fs = FileSystem.get(conf);
			isFileExist = fs.exists(new Path(dest));
		} catch (Exception e) {
		}

		return isFileExist;

	}

	public static boolean copyFile2Local(String dest, String local) {

		Configuration conf = new Configuration();
		try {
			// FileSystem fs = FileSystem.get(URI.create(dest), conf);
			FileSystem fs = FileSystem.get(conf);
			boolean isFileExist = fs.exists(new Path(dest));
			System.out.println(isFileExist);
			if (!isFileExist)
				return false;

			FSDataInputStream fsdi = fs.open(new Path(dest));
			OutputStream output = new FileOutputStream(local);
			IOUtils.copyBytes(fsdi, output, 4096, true);
			fs.close();
		} catch (IOException e) {
			return false;
		}

		return true;

	}

	public static Set<RelationFansInfo> readFans(String HDFSPath, Long hId) throws IOException {
		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		Path path = new Path(HDFSPath);
		FSDataInputStream fsDataInputStream = hdfs.open(path);
		Set<RelationFansInfo> fans = new HashSet<RelationFansInfo>();

		String line;
		int i = 0;
		while ((line = fsDataInputStream.readLine()) != null && i < 21) {
			try {
				if (i == 0) {
					i++;
					continue;
				}
				String fansInfo[] = line.split(",");
				if (line.replace(" ", "").length() != 0 && fansInfo.length >= 5) {
					RelationFansInfo r = new RelationFansInfo();
					r.setPageLink("https://www.facebook.com/" + fansInfo[0]);
					r.setUserName(fansInfo[1]);
					r.setAvatarUrl(fansInfo[2]);
					r.setHistoryId(hId);
					fans.add(r);
					i++;
				} else {
					continue;
				}
			} catch (Exception ex) {
				continue;
			}

		}
		fsDataInputStream.close();
		hdfs.close();
		// IOUtils.copyBytes(fsDataInputStream, null, conf, true);
		return fans;
	}

	public static boolean deleteHDFSFile(String dst) {
		Configuration config = new Configuration();
		FileSystem hdfs = null;
		boolean isDeleted = false;
		try {
			hdfs = FileSystem.get(config);

			Path path = new Path(dst);
			isDeleted = hdfs.delete(path, true);
			hdfs.deleteOnExit(path);
			hdfs.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (hdfs != null)
				try {
					hdfs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

		return isDeleted;
	}

}
