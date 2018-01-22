package com.sa.storm.definition;

public class CADefinition {

	public enum CounterRollingRange {

		ONE_DAY(1), ONE_WEEK(7), ONE_MONTH(30), THREE_MONTH(60), ONE_YEAR(365);

		private int numDays;

		private CounterRollingRange(int numDays) {
			this.numDays = numDays;
		}

		public int getNumDays() {
			return numDays;
		}

	}

	public enum CounterType {

		NUM_POST(1), NUM_COMMENT(2), NUM_PAGE_LIKE(3), NUM_POST_LIKE(4), NUM_COMMENT_LIKE(5), NUM_POST_SHARE(6);

		private int value;

		private CounterType(int value) {
			this.value = value;
		}

		public int getValue() {
			return value;
		}

	}

}
