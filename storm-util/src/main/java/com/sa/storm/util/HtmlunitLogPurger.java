/**
 * 
 */
package com.sa.storm.util;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author lewis
 * 
 */
public class HtmlunitLogPurger {

	private static final Log log = LogFactory.getLog(HtmlunitLogPurger.class);

	private static HtmlunitLogPurger instance = null;

	private static boolean runPurger = true;

	// 24 hours
	private int minsBefore = 1440;

	public HtmlunitLogPurger() {
		log.debug("Initialize HtmlunitLogPurger");
	}

	public void doPurge() {
		final Thread t = new Thread(new Runnable() {
			public void run() {
				if (isRunPurger()) {
					try {
						cleanup();
					} catch (Exception e) {
						log.error(e.getMessage(), e);
					}
				}
			}
		});
		t.start();
	}

	// public static HtmlunitLogPurger getInstance() {
	// if (instance == null) {
	// synchronized (HtmlunitLogPurger.class) {
	// if (instance == null) {
	// instance = new HtmlunitLogPurger();
	// }
	// }
	// }
	// return instance;
	// }

	void cleanup() throws Exception {
		String tempDir = System.getProperty("java.io.tmpdir");
		log.info("tempdir: " + tempDir);

		deleteFilesOlderThanNminutes(minsBefore, tempDir);
	}

	void deleteFilesOlderThanNminutes(int minsBack, String tempPath) throws Exception {

		final File directory = new File(tempPath);
		if (directory.exists()) {
			File[] listFiles = directory.listFiles();
			long purgeTime = System.currentTimeMillis() - (minsBack * 60 * 1000);
			for (File listFile : listFiles) {
				try {
					if (listFile.getName().contains("htmlunit")) {
						if (listFile.lastModified() < purgeTime) {
							if (!listFile.delete()) {
								log.warn("Unable to delete file: " + listFile);
							}
						}
					}
				} catch (Exception e) {
					log.error(e.getMessage(), e);
				}
			}
		} else {
			log.warn("Files were not deleted, directory " + tempPath + " doesn't exist!");
		}

	}

	/**
	 * @return the runPurger
	 */
	static synchronized boolean isRunPurger() {
		return runPurger;
	}

	/**
	 * @param runPurger
	 *            the runPurger to set
	 */
	static synchronized void setRunPurger(boolean runPurger) {
		HtmlunitLogPurger.runPurger = runPurger;
	}

	public void enable() {
		setRunPurger(true);
	}

	public void disable() {
		setRunPurger(false);
	}
}
