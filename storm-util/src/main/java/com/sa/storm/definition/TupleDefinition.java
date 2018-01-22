package com.sa.storm.definition;

import org.apache.commons.lang3.StringUtils;

public class TupleDefinition {

	public static enum Field {
		REQUEST_ID("redId"), PARENT_REQUEST_ID("pReqId"), SOURCE_TYPE("sourceType");

		private final String code;

		private Field(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}

		@Override
		public String toString() {
			return getCode();
		}
	}

	/**
	 * IGNORE_LANG is currently used by twitter only
	 * 
	 */
	public static enum Param {
		PARENT_DOC_ID("rID"), SEED_ID("seedId"), REGULAR_ID("regularId"), PAGE_NUM("pageNum"), KEYWORD("keyword"), USER_ID("uId"), URL(
				"url"), LOGIN_USER_KEY("loginUserKey"), LOGIN_USER_TYPE("loginUserType"), IGNORE_LANG("ignoreLang"), START_RECORD_TWEET(
				"srt"), DOCUMENTS("docs"), START_TIME("startTime"), END_TIME("endTime"), DATE("date"), HASHTAG_ID("hashtagId"), UNIT_TYPE("unitType"), NEW_PAGE("newPage"),PAGE_ID("pageId"), PAGE_FANS("fans"), BATCH_DOC_INFO("batchDocInfo"), IS_GROUP("isGroup");

		private final String code;

		private Param(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}

		@Override
		public String toString() {
			return getCode();
		}
	}

	public static enum Context {
		START_PROCESS_TIME("startProcessTime"), RETRY_COUNT("retryCount"), LAST_RECORD_TWEET("lrt"), RETWEET_NUM("retweetNum"), COMMENT_NUM(
				"commentNum"), ERROR_MESSAGE("errorMessage"), NEW_DOCS_NUM("newDocsNum"), UPDATE_DOCS_NUM("updateDocsNum"), SITE_INFO(
				"siteInfo"), DETAIL_URL("detailUrl"), CRAWLED_CONTENT("crawledContent"), PAGE_NUM("pageNum"), TARGET_PAGE("targetPage"), DOCUMENT_ID(
				"docId"), PROFILE_IDS("profileIds"), PAGE_SIZE("pageSize"), HOT_TOPIC_CAT_ID("hotTopicCatId"), HOT_TOPIC_CAT_NAME(
				"hotTopicCatName"), HOT_TOPIC_HASHTAG_ID("hotTopicHashtagId"), HOT_TOPIC_HASHTAG_NAME("hotTopicHashtagName");

		private final String code;

		private Context(String code) {
			this.code = code;
		}

		public String getCode() {
			return code;
		}

		@Override
		public String toString() {
			return getCode();
		}
	}

	public static enum Result {
		FAIL(0), SUCCESS(1);

		private final int code;

		private Result(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}

		@Override
		public String toString() {
			return String.valueOf(getCode());
		}
	}

	public static enum UnitType {
		DEMO(0), PRODUCTION(1);

		private final int code;

		private UnitType(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}

		public static UnitType getUnitType(int unitTypeCode) {
			for (UnitType unitType : UnitType.values()) {
				if (unitType.getCode() == unitTypeCode) {
					return unitType;
				}
			}
			return null;
		}

		public static UnitType getUnitType(String unitTypeStr) {
			if (!StringUtils.isEmpty(unitTypeStr)) {
				return getUnitType(Integer.parseInt(unitTypeStr));
			}

			return null;
		}

		@Override
		public String toString() {
			return String.valueOf(getCode());
		}
	}
}
