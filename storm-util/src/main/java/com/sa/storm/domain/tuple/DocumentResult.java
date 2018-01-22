package com.sa.storm.domain.tuple;

import java.util.Map;

import com.sa.storm.domain.persistence.Document;

public class DocumentResult extends BaseTuple {

	private static final long serialVersionUID = 1L;

	private Document document;

	public DocumentResult() {
		super();
	}

	public DocumentResult(BaseTuple original) {
		super(original);
	}

	public DocumentResult(String requestId, Integer type, String sourceType, Map<String, String> params) {
		super(requestId, type, sourceType, params);
	}

	public Document getDocument() {
		return document;
	}

	public void setDocument(Document document) {
		this.document = document;
	}
}
