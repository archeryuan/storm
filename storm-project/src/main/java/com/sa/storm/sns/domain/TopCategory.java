/**
 *Copyright(C) ©2012 SocialAnalytics. All rights reserved.
 *
 */
package com.sa.storm.sns.domain;


/**
 * @description
 * Top category object.
 * @author                            Luke
 * @created date                      2014-12-11 
 * @modification history<BR>
 * No.        Date          Modified By             <B>Why & What</B> is modified  
 *
 * @see                               
 */
public class TopCategory {
	
	private String categoryName;
	
	private long interestFans;

	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}

	public long getInterestFans() {
		return interestFans;
	}

	public void setInterestFans(long interestFans) {
		this.interestFans = interestFans;
	}

	public void addInterestFans(long count) {
		this.interestFans += count;
	}
	
	public void decrInterestFans(long count) {
		this.interestFans -= count;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((categoryName == null) ? 0 : categoryName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TopCategory other = (TopCategory) obj;
		if (categoryName == null) {
			if (other.categoryName != null)
				return false;
		} else if (!categoryName.equals(other.categoryName))
			return false;
		return true;
	}
	

}
