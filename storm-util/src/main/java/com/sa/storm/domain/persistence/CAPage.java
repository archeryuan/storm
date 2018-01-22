package com.sa.storm.domain.persistence;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.sa.common.definition.SourceType;

public class CAPage extends BasePersistenceObject implements Serializable {

	private static final long serialVersionUID = 4260363218100774806L;

	public static final String HBASE_TABLE = "capage";

	public static final byte[] HBASE_INFO_COLUMN_FAMILY = Bytes.toBytes("i");

	public static final byte[] HBASE_NUM_LIKE = Bytes.toBytes("numLike");

	public static final byte[] HBASE_NUM_LIKE_DATE = Bytes.toBytes("numLikeDate");

	public static final byte[] HBASE_SUBSCRIBER_COUNT_COUNT = Bytes.toBytes("subscriberCount");

	public static final byte[] HBASE_CATEGORY = Bytes.toBytes("category");

	private String id;

	private String name;

	private String category;

	private SourceType sourceType;

	private Long numLike;

	private Date numLikeDate;

	private Long totalViews;

	private Date totalViewsDate;

	private Long subscriberCount;

	private Date subscriberCountDate;

	private String crawlDate;
	
	private boolean isGroup;

	public CAPage() {

	}

	public CAPage(Date currentTime) {
		SimpleDateFormat formatter = new SimpleDateFormat("yy-MM-dd");
		this.crawlDate = formatter.format(currentTime);
	}

	public CAPage(Result result) {
		if (result == null) {
			return;
		}

		final String rowKey = Bytes.toString(result.getRow());
		if ((rowKey == null) || rowKey.isEmpty()) {
			return;
		}

		final String sourceTypeStr = rowKey.split("#")[0];
		final String id = rowKey.split("#")[2];
		final Long numLike = getLongFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE);
		final Date numLikeDate = getDateFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE_DATE);
		final String category = getStringFromResult(result, HBASE_INFO_COLUMN_FAMILY, HBASE_CATEGORY);

		this.id = id;
		this.sourceType = SourceType.getSourceType(sourceTypeStr);
		this.numLike = numLike;
		this.numLikeDate = numLikeDate;
		this.category = category;

	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public SourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}

	public Long getNumLike() {
		return numLike;
	}

	public void setNumLike(Long numLike) {
		this.numLike = numLike;
	}

	public Date getNumLikeDate() {
		return numLikeDate;
	}

	public void setNumLikeDate(Date numLikeDate) {
		this.numLikeDate = numLikeDate;
	}

	public Long getTotalViews() {
		return totalViews;
	}

	public void setTotalViews(Long totalViews) {
		this.totalViews = totalViews;
	}

	public Date getTotalViewsDate() {
		return totalViewsDate;
	}

	public void setTotalViewsDate(Date totalViewsDate) {
		this.totalViewsDate = totalViewsDate;
	}

	public Long getSubscriberCount() {
		return subscriberCount;
	}

	public void setSubscriberCount(Long subscriberCount) {
		this.subscriberCount = subscriberCount;
	}

	public Date getSubscriberCountDate() {
		return subscriberCountDate;
	}

	public void setSubscriberCountDate(Date subscriberCountDate) {
		this.subscriberCountDate = subscriberCountDate;
	}

	public String getCrawlDate() {
		return crawlDate;
	}

	public void setCrawlDate(String crawlDate) {
		this.crawlDate = crawlDate;
	}

	public boolean isGroup() {
		return isGroup;
	}

	public void setGroup(boolean isGroup) {
		this.isGroup = isGroup;
	}

	public Put toHBasePut() {
		Put put = new Put(Bytes.toBytes(generateHBaseRowKey(sourceType, id)));
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE, numLike);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_NUM_LIKE_DATE, numLikeDate);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_SUBSCRIBER_COUNT_COUNT, subscriberCount);
		addFieldToPut(put, HBASE_INFO_COLUMN_FAMILY, HBASE_CATEGORY, category);
		return put;
	}

	public static String generateHBaseRowKey(SourceType sourceType, String pageId) {
		Date currentTime = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yy-MM-dd");
		return new StringBuilder(sourceType.getSourceTypeStr()).append("#").append(formatter.format(currentTime)).append("#")
				.append(pageId).toString();
	}

	public static String generateHBaseRowKey(SourceType sourceType, Date date, String pageId) {
		SimpleDateFormat formatter = new SimpleDateFormat("yy-MM-dd");
		return new StringBuilder(sourceType.getSourceTypeStr()).append("#").append(formatter.format(date)).append("#").append(pageId)
				.toString();
	}

	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
