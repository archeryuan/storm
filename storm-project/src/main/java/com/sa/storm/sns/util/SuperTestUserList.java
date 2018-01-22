package com.sa.storm.sns.util;

import java.util.ArrayList;

public class SuperTestUserList {

	static ArrayList<String> userList = new ArrayList<String>();
	
	public static ArrayList<String> getSuperUserList(){
		userList.add("4");
		userList.add("5");
		userList.add("6");
		userList.add("7");
		userList.add("9");
		userList.add("19");
		userList.add("54");
		return userList;
	}
	
}
