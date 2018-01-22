/**
 * 
 */
package com.sa.storm.domain.tuple;

import java.util.Map;

import social.hunt.storm.domain.tuple.solr.SolrInputDocumentMap;

/**
 * @author lewis
 *
 */
public class SolrInputDocumentBatchResult extends BaseTuple {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5193684438575415893L;
	private SolrInputDocumentMap documentMap;

	/**
	 * 
	 */
	public SolrInputDocumentBatchResult() {
	}

	/**
	 * @param requestId
	 * @param type
	 * @param sourceType
	 * @param params
	 */
	public SolrInputDocumentBatchResult(String requestId, Integer type, String sourceType, Map<String, String> params) {
		super(requestId, type, sourceType, params);
	}

	/**
	 * @param requestId
	 * @param parentRequestId
	 * @param type
	 * @param sourceType
	 * @param params
	 */
	public SolrInputDocumentBatchResult(String requestId, String parentRequestId, Integer type, String sourceType,
			Map<String, String> params) {
		super(requestId, parentRequestId, type, sourceType, params);
	}

	/**
	 * @param requestId
	 * @param parentRequestId
	 * @param type
	 * @param sourceType
	 * @param priority
	 * @param params
	 */
	public SolrInputDocumentBatchResult(String requestId, String parentRequestId, Integer type, String sourceType, Integer priority,
			Map<String, String> params) {
		super(requestId, parentRequestId, type, sourceType, priority, params);
	}

	/**
	 * @param original
	 */
	public SolrInputDocumentBatchResult(BaseTuple original) {
		super(original);
	}

	/**
	 * @param requestId
	 * @param original
	 */
	public SolrInputDocumentBatchResult(String requestId, BaseTuple original) {
		super(requestId, original);
	}

	/**
	 * @return the documentMap
	 */
	public SolrInputDocumentMap getDocumentMap() {
		return documentMap;
	}

	/**
	 * @param documentMap
	 *            the documentMap to set
	 */
	public void setDocumentMap(SolrInputDocumentMap documentMap) {
		this.documentMap = documentMap;
	}

}
