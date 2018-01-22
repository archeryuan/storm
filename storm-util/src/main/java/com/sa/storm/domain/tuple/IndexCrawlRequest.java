/**
 * 
 */
package com.sa.storm.domain.tuple;

import java.util.Map;

/**
 * @author lewis
 * 
 */
public class IndexCrawlRequest extends BaseTuple {

	private static final long serialVersionUID = 1L;

	/**
	 * 
	 */
	public IndexCrawlRequest() {
	}

	/**
	 * @param original
	 */
	public IndexCrawlRequest(BaseTuple original) {
		super(original);
	}

	/**
	 * @param requestId
	 * @param type
	 * @param sourceType
	 * @param params
	 */
	public IndexCrawlRequest(String requestId, Integer type, String sourceType,
			Map<String, String> params) {
		super(requestId, type, sourceType, params);
	}

}
