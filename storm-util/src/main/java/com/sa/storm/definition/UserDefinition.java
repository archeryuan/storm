package com.sa.storm.definition;

public class UserDefinition {

	public static enum Gender {
		MALE("M"), FEMALE("F");

		private String code;

		private Gender(String code) {
			this.code = code;
		}

		public String getValue() {
			return code;
		}

		@Override
		public String toString() {
			return getValue();
		}

	}
}
