package com.sa.storm.domain.persistence;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class DocumentIndex extends BasePersistenceObject {

	public static final String HBASE_INDEX_TABLE = "di";
	public static final byte[] HBASE_INDEX_FAMILY_I = Bytes.toBytes("i");

	public static final byte[] docIdCol = Bytes.toBytes("key");
	public static final byte[] userIdCol = Bytes.toBytes("uId");

	private String rowKey;

	private String docId;

	private String userId;

	public DocumentIndex(String docId, String twitterId, String sourceType, String userId) {
		this.docId = docId;
		this.rowKey = DocumentIndex.generateHBaseIndexKey(sourceType, twitterId);
		this.userId = userId;
	}

	public DocumentIndex(Document doc) {
		this(doc.getId(), doc.getTwitterId(), doc.getSourceType().getSourceTypeStr(), doc.getUserId());
	}

	public DocumentIndex(Result result) {
		this.fromHBaseResult(result);
	}
    
	public DocumentIndex(String docId, String rowKey, String userId) {
		this.docId = docId;
		this.rowKey = rowKey;
		this.userId = userId;
	}
	
	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	/**
	 * Generate a HBase Put for updating index
	 * 
	 * @return
	 */
	public Put toHBaseIndexPut() {
		Put put = new Put(Bytes.toBytes(rowKey));
		addFieldToPut(put, HBASE_INDEX_FAMILY_I, docIdCol, this.docId);
		addFieldToPut(put, HBASE_INDEX_FAMILY_I, userIdCol, this.userId);

		return put;
	}

	public static String generateHBaseIndexKey(String sourceTypeStr, String twitterId) {
		return new StringBuffer(sourceTypeStr).append("_").append(twitterId).toString();
	}

	protected void fromHBaseResult(Result result) {
		if (result != null && !result.isEmpty()) {
			rowKey = Bytes.toString(result.getRow());
			docId = getStringFromResult(result, DocumentIndex.HBASE_INDEX_FAMILY_I, DocumentIndex.docIdCol);
			userId = getStringFromResult(result, DocumentIndex.HBASE_INDEX_FAMILY_I, DocumentIndex.userIdCol);
		}
	}
}
