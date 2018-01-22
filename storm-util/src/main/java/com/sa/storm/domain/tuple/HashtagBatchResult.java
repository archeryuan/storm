package com.sa.storm.domain.tuple;

import java.util.ArrayList;
import java.util.Collection;

import com.sa.storm.domain.persistence.Hashtag;

public class HashtagBatchResult extends BaseTuple {

	private static final long serialVersionUID = 1L;

	private Collection<Hashtag> hashtags;

	public HashtagBatchResult() {

	}

	public HashtagBatchResult(BaseTuple original) {
		super(original);
	}

	public Collection<Hashtag> getHashtags() {
		return hashtags;
	}

	public void setHashtags(Collection<Hashtag> hashtags) {
		this.hashtags = hashtags;
	}

	public void addHashtag(Hashtag hashtag) {
		if (hashtags == null) {
			hashtags = new ArrayList<Hashtag>();
		}

		hashtags.add(hashtag);
	}
}
