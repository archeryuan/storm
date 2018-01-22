package com.sa.storm.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sa.common.hbase.HBaseConnector;

public class HBaseUtil {

	private static final Logger log = LoggerFactory.getLogger(HBaseUtil.class);

	private static HBaseUtil instance;

	private HBaseUtil() {
	}

	public static HBaseUtil getInstance() {
		if (instance == null) {
			synchronized (HBaseUtil.class) {
				if (instance == null)
					instance = new HBaseUtil();
			}
		}
		return instance;
	}

	public Result executeGet(String tableName, Get get) throws IOException {
		HTableInterface table = getTable(tableName);
		Result result = null;
		try {
			result = table.get(get);
		} finally {
			closeTable(table);
		}

		return result;
	}

	public HashMap<String, String> executeGetListForHashMap(String tableName, List<Get> getList) throws IOException {

		HTableInterface table = getTable(tableName);
		Result[] resultList = (table == null ? null : table.get(getList));
		HashMap<String, String> resultMap = new HashMap<String, String>();
		try {
			for (Result r : resultList) {
				String id = Bytes.toString(r.getRow());
				Long numLike = getLongFromResult(r, Bytes.toBytes("d"), Bytes.toBytes("numLike")) == null ? -1 : getLongFromResult(r,
						Bytes.toBytes("d"), Bytes.toBytes("numLike"));
				Long comCount = getLongFromResult(r, Bytes.toBytes("d"), Bytes.toBytes("comCount")) == null ? -1 : getLongFromResult(r,
						Bytes.toBytes("d"), Bytes.toBytes("comCount"));
				Long retCount = getLongFromResult(r, Bytes.toBytes("d"), Bytes.toBytes("retCount")) == null ? -1 : getLongFromResult(r,
						Bytes.toBytes("d"), Bytes.toBytes("retCount"));
				Long viewCount = getLongFromResult(r, Bytes.toBytes("d"), Bytes.toBytes("viewCount")) == null ? -1 : getLongFromResult(r,
						Bytes.toBytes("d"), Bytes.toBytes("viewCount"));
				resultMap.put(id, numLike + "_" + comCount + "_" + retCount + "_" + viewCount);
			}
		} finally {
			closeTable(table);
		}
		return resultMap;
	}

	public Result[] executeBatchGet(String tableName, List<Get> getList) throws IOException {
		if (StringUtils.isEmpty(tableName) || null == getList || getList.size() <= 0)
			return null;

		HTableInterface table = getTable(tableName);
		Result[] resultList = null;

		try {
			resultList = table.get(getList);
		} finally {
			closeTable(table);
		}
		return resultList;
	}

	private byte[] getBytesFromResult(Result result, byte[] family, byte[] qualifier) {
		if (result != null) {
			return result.getValue(family, qualifier);
		}

		return null;
	}

	protected Long getLongFromResult(Result result, byte[] family, byte[] qualifier) {
		byte[] rawValue = getBytesFromResult(result, family, qualifier);
		if (rawValue != null) {
			return Bytes.toLong(rawValue);
		}
		return null;
	}

	@Deprecated
	public ResultScanner executeScan(String tableName, Scan scan) throws IOException {
		HTableInterface table = getTable(tableName);
		ResultScanner resultScanner = null;
		resultScanner = table.getScanner(scan);
		return resultScanner;
	}

	public void executePut(String tableName, Put put) throws IOException {
		HTableInterface table = getTable(tableName);
		try {
			table.put(put);
		} finally {
			closeTable(table);
		}
	}

	public void executePut(String tableName, List<Put> puts) throws IOException {
		HTableInterface table = getTable(tableName);
		try {
			table.put(puts);
		} finally {
			closeTable(table);
		}
	}

	/**
	 * Atomically check the existence of a row and put to HBase if exists
	 * 
	 * @param tableName
	 * @param family
	 * @param col
	 * @param rowKey
	 * @param put
	 * @return
	 * @throws IOException
	 */
	public boolean executePutIfNotExists(String tableName, byte[] family, byte[] col, byte[] rowKey, Put put) throws IOException {

		HTableInterface table = getTable(tableName);

		try {
			return table.checkAndPut(rowKey, family, col, null, put);
		} finally {
			closeTable(table);
		}
	}

	public void executeDelete(String tableName, List<Delete> deletes) throws IOException {
		HTableInterface table = getTable(tableName);
		try {
			table.delete(deletes);
		} finally {
			closeTable(table);
		}
	}

	public HTableInterface getTable(String tableName) throws IOException {
		return HBaseConnector.getInstance().getTable(Bytes.toBytes(tableName));
	}

	private void closeTable(HTableInterface table) throws IOException {
		if (table != null) {
			table.close();
		}
	}
}
