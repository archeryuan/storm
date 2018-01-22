package com.sa.storm.domain.persistence;

import org.apache.hadoop.hbase.util.Bytes;

public class ImageIndex {
	public static final String HBASE_IMAGE_INDEX_TABLE = "i";
	public static final byte[] HBASE_NEWS_INDEX_FAMILY_I = Bytes.toBytes("i");
	public static final byte[] COLUMN_CDATE = Bytes.toBytes("cDate");
}
