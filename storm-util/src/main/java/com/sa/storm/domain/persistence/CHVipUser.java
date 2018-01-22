package com.sa.storm.domain.persistence;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class CHVipUser extends BasePersistenceObject implements Serializable {

	private static final long serialVersionUID = -4379423821331624372L;

	public static final String HBASE_TABLE = "vu";

	public static final byte[] HBASE_INFO_COLUMN_FAMILY = Bytes.toBytes("i");

	public static final byte[] HBASE_CREATE_DATE = Bytes.toBytes("a");

	private String id;

	private Date createDate;

	public CHVipUser(String email) {
		this.id = email;
		this.createDate = new Date();;
	}

	public CHVipUser(Result result) {
		if (result == null) {
			return;
		}

		final String rowKey = Bytes.toString(result.getRow());
		if ((rowKey == null) || rowKey.isEmpty()) {
			return;
		}

		final String id = rowKey;
		final Date createDate = getDateFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE);

		this.id = id;
		this.createDate = createDate;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Date getCreateDate() {
		return createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

	public Put toHBasePut() {
		Put put = new Put(Bytes.toBytes(id));
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_CREATE_DATE, createDate);
		return put;
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
