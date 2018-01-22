package com.sa.storm.definition;

public class DocumentDefinition {

	public static enum TweetType {
		ORIGINAL(1), RETWEET(2), COMMENT(3), LIKE(4);

		private int code;

		private TweetType(int code) {
			this.code = code;
		}

		public int getValue() {
			return code;
		}

		@Override
		public String toString() {
			return String.valueOf(getValue());
		}
	}
}
