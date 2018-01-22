package com.sa.storm.domain.tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.sa.storm.domain.persistence.Document;

public class DocumentBatchResult extends BaseTuple {

	private static final long serialVersionUID = 1L;

	private Collection<Document> documents;

	public DocumentBatchResult() {
		super();
	}

	public DocumentBatchResult(BaseTuple original) {
		super(original);
	}

	public DocumentBatchResult(String requestId, Integer type, String sourceType, Map<String, String> params) {
		super(requestId, type, sourceType, params);
	}

	public Collection<Document> getDocuments() {
		return documents;
	}

	public void setDocuments(Collection<Document> documents) {
		this.documents = documents;
	}

	public void addDocument(Document document) {
		if (documents == null) {
			documents = new ArrayList<Document>();
		}

		documents.add(document);
	}
}
