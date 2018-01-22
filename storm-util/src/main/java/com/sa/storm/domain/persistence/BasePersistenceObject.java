package com.sa.storm.domain.persistence;

import java.util.Date;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class BasePersistenceObject {

	protected Put addFieldToPut(Put put, byte[] family, byte[] column, String value) {
		if (value != null && !value.isEmpty()) {
			put.add(family, column, Bytes.toBytes(value));
		}
		return put;
	}

	protected Put addFieldToPut(Put put, byte[] family, byte[] column, Date value) {
		if (value != null) {
			return this.addFieldToPut(put, family, column, value.getTime());
		}
		return put;
	}

	protected Put addFieldToPut(Put put, byte[] family, byte[] column, Long value) {
		if (value != null) {
			put.add(family, column, Bytes.toBytes(value));
		}
		return put;
	}

	protected Put addFieldToPut(Put put, byte[] family, byte[] column, Integer value) {
		if (value != null) {
			put.add(family, column, Bytes.toBytes(value));
		}
		return put;
	}

	protected Put addFieldToPut(Put put, byte[] family, byte[] column, Float value) {
		if (value != null) {
			put.add(family, column, Bytes.toBytes(value));
		}
		return put;
	}

	protected Put addFieldToPut(Put put, byte[] family, byte[] column, Boolean value) {
		if (value != null) {
			put.add(family, column, Bytes.toBytes(value));
		}
		return put;
	}

	protected Long getLongFromResult(Result result, byte[] family, byte[] qualifier) {
		byte[] rawValue = getBytesFromResult(result, family, qualifier);
		if (rawValue != null) {
			return Bytes.toLong(rawValue);
		}
		return null;
	}

	protected String getStringFromResult(Result result, byte[] family, byte[] qualifier) {
		byte[] rawValue = getBytesFromResult(result, family, qualifier);
		if (rawValue != null) {
			return Bytes.toString(rawValue);
		}
		return null;
	}

	protected Date getDateFromResult(Result result, byte[] family, byte[] qualifier) {
		final Long time = getLongFromResult(result, family, qualifier);
		if (time != null) {
			return new Date(time);
		}
		return null;
	}

	protected Integer getIntegerFromResult(Result result, byte[] family, byte[] qualifier) {
		byte[] rawValue = getBytesFromResult(result, family, qualifier);
		if (rawValue != null) {
			return Bytes.toInt(rawValue);
		}
		return null;
	}

	protected Float getFloatFromResult(Result result, byte[] family, byte[] qualifier) {
		byte[] rawValue = getBytesFromResult(result, family, qualifier);
		if (rawValue != null) {
			return Bytes.toFloat(rawValue);
		}
		return null;
	}
	
	protected Boolean getBooleanFromResult(Result result, byte[] family, byte[] qualifier) {
		byte[] rawValue = getBytesFromResult(result, family, qualifier);
		if (rawValue != null) {
			return Bytes.toBoolean(rawValue);
		}
		return null;
	}

	private byte[] getBytesFromResult(Result result, byte[] family, byte[] qualifier) {
		if (result != null) {
			return result.getValue(family, qualifier);
		}

		return null;
	}
}
