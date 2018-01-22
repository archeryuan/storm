package com.sa.storm.domain.persistence;

import org.apache.hadoop.hbase.util.Bytes;

public class NewsIndex {
	public static final String HBASE_NEWS_INDEX_TABLE = "e";
	public static final byte[] HBASE_NEWS_INDEX_FAMILY_I = Bytes.toBytes("t");
	public static final byte[] HBASE_NEWS_INDEX_QUALI = Bytes.toBytes("e");
	public static final byte[] COLUMN_PUBLISH_DATE = Bytes.toBytes("pDate");
	public static final byte[] COLUMN_LAST_CRAWL_DATE = Bytes.toBytes("lcd");
}
