/**
 *Copyright(C) ©2012 SocialAnalytics. All rights reserved.
 *
 */
package com.sa.storm.sns.domain;

import java.util.Comparator;


/**
 * @description
 * KeyValuePair Comparator sorting by value asc, i.e, the first element is whose value is the minimum.
 * @author                            Luke
 * @created date                      2014-12-10 
 * @modification history<BR>
 * No.        Date          Modified By             <B>Why & What</B> is modified  
 *
 * @see                               
 */
public class StatisticObjectComparator implements Comparator<StatisticObject> {

	@Override
	public int compare(StatisticObject o1, StatisticObject o2) {
		long v1 = o1.getCount();
		long v2 = o2.getCount();
		
		if (v1 > v2)
			return 1;
		else if(v1 < v2)
			return -1;
		else
			return 0;	
	
	}

}
