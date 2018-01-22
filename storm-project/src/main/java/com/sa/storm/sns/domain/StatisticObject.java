/**
 *Copyright(C) ©2012 SocialAnalytics. All rights reserved.
 *
 */
package com.sa.storm.sns.domain;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

/**
 * @description
 * StatisticObject.
 * @author                            Luke
 * @created date                      2014-12-10 
 * @modification history<BR>
 * No.        Date          Modified By             <B>Why & What</B> is modified  
 *
 * @see                               
 */
public class StatisticObject {
	
	private Map<String, Object> params;
	
	private long count;	
	
	public StatisticObject() {
		params = new HashMap<String, Object>();
		count = 0L;
	}
	
	public Object get(String paramName) {
		return params.get(paramName);
	}

	public void add(String paramName, Object value) {
		if (!StringUtils.isEmpty(paramName) && null != value)
			params.put(paramName, value);
		
	}
	
	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	
	

}
