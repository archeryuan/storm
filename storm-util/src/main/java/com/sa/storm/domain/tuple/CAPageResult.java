package com.sa.storm.domain.tuple;

import com.sa.storm.domain.persistence.CAPage;

public class CAPageResult extends BaseTuple {

	private static final long serialVersionUID = -4749014884693778602L;

	private CAPage page;

	public CAPageResult() {

	}

	public CAPageResult(BaseTuple original) {
		super(original);
	}

	public CAPage getPage() {
		return page;
	}

	public void setPage(CAPage page) {
		this.page = page;
	}

}
