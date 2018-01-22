/**
 * 
 */
package com.sa.storm.domain.tuple;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import com.sa.storm.spider.tuple.BaseTuple;

/**
 * @author lewis
 * 
 */
public class ProfileTuple extends BaseTuple {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3379952520151930375L;
	private long profileId;

	/**
	 * 
	 */
	public ProfileTuple() {
	}

	/**
	 * @param tuple
	 */
	public ProfileTuple(BaseTuple tuple) {
		super(tuple);
	}

	/**
	 * @param taskType
	 * @param priority
	 */
	public ProfileTuple(int taskType, int priority, long profileId) {
		super(taskType, priority);
		this.profileId = profileId;
	}

	/**
	 * @return the profileId
	 */
	public long getProfileId() {
		return profileId;
	}

	/**
	 * @param profileId
	 *            the profileId to set
	 */
	public void setProfileId(long profileId) {
		this.profileId = profileId;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (profileId ^ (profileId >>> 32));
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		ProfileTuple other = (ProfileTuple) obj;
		if (profileId != other.profileId)
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ReflectionToStringBuilder.toString(this);
	}

}
