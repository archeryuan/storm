package social.hunt.storm.domain.tuple;

import java.util.Map;

import social.hunt.storm.domain.tuple.solr.SolrInputDocumentMap;

import com.sa.storm.domain.tuple.BaseTuple;

public class SemantriaTaskResult extends BaseTuple{

	/**
	 * 
	 */
	private static final long serialVersionUID = -649628178707251168L;
	private SolrInputDocumentMap documentMap;

	/**
	 * 
	 */
	public SemantriaTaskResult() {
	}

	/**
	 * @param requestId
	 * @param type
	 * @param sourceType
	 * @param params
	 */
	public SemantriaTaskResult(String requestId, Integer type, String sourceType, Map<String, String> params) {
		super(requestId, type, sourceType, params);
	}

	/**
	 * @param requestId
	 * @param parentRequestId
	 * @param type
	 * @param sourceType
	 * @param params
	 */
	public SemantriaTaskResult(String requestId, String parentRequestId, Integer type, String sourceType, Map<String, String> params) {
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
	public SemantriaTaskResult(String requestId, String parentRequestId, Integer type, String sourceType, Integer priority,
									Map<String, String> params) {
		super(requestId, parentRequestId, type, sourceType, priority, params);
	}

	/**
	 * @param original
	 */
	public SemantriaTaskResult(BaseTuple original) {
		super(original);
	}

	public SemantriaTaskResult(BaseTuple original, SolrInputDocumentMap documentMap) {
		super(original);
		this.documentMap = documentMap;
	}

	/**
	 * @param requestId
	 * @param original
	 */
	public SemantriaTaskResult(String requestId, BaseTuple original) {
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
