package com.sa.storm.domain.tuple;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.sa.crawler.definition.Message;
import com.sa.crawler.message.TaskResultMessage;
import com.sa.storm.definition.TupleDefinition;

public class TaskResult extends BaseTuple {

	private static final long serialVersionUID = 1L;

	private String message;

	private String exceptionMessage;

	private StackTraceElement[] exceptionStackTrace;

	public TaskResult() {
		super();
	}

	public TaskResult(BaseTuple original, int resultCode) {
		super(original);
		setResultCode(resultCode);
	}

	public TaskResult(String requestId, BaseTuple original, int resultCode) {
		super(requestId, original);
		setResultCode(resultCode);
	}

	public TaskResult(BaseTuple original, String message, int resultCode) {
		this(original, resultCode);
		this.message = message;
	}

	public TaskResult(BaseTuple original, String message, Exception exception, int resultCode) {
		this(original, resultCode);
		this.message = message;
		this.exceptionMessage = exception.getMessage();
		this.exceptionStackTrace = exception.getStackTrace();
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getExceptionMessage() {
		return exceptionMessage;
	}

	public void setExceptionMessage(String exceptionMessage) {
		this.exceptionMessage = exceptionMessage;
	}

	public StackTraceElement[] getExceptionStackTrace() {
		return exceptionStackTrace;
	}

	public void setExceptionStackTrace(StackTraceElement[] exceptionStackTrace) {
		this.exceptionStackTrace = exceptionStackTrace;
	}

	public TaskResultMessage toMessage() {
		TaskResultMessage message = new TaskResultMessage();

		final String parentRequestId = getParentRequestId();
		if (parentRequestId == null || parentRequestId.isEmpty()) {
			message.setId(getRequestId());
		} else {
			message.setId(getParentRequestId());
		}

		message.setResultCode(getResultCode());

		String newDocsNumStr = getContextByKey(TupleDefinition.Context.NEW_DOCS_NUM);
		if (newDocsNumStr != null && !newDocsNumStr.isEmpty()) {
			message.setNumOfNewDocs(Integer.parseInt(newDocsNumStr));
		}

		Map<String, String> states = new HashMap<String, String>();
		if (getContextByKey(TupleDefinition.Context.LAST_RECORD_TWEET) != null) {
			states.put(Message.State.LAST_RECORD_TWEET.getCode(), getContextByKey(TupleDefinition.Context.LAST_RECORD_TWEET));
		}
		if (getContextByKey(TupleDefinition.Context.COMMENT_NUM) != null) {
			states.put(Message.State.COMMENT_NUM.getCode(), getContextByKey(TupleDefinition.Context.COMMENT_NUM));
		}
		if (getContextByKey(TupleDefinition.Context.RETWEET_NUM) != null) {
			states.put(Message.State.RETWEET_NUM.getCode(), getContextByKey(TupleDefinition.Context.RETWEET_NUM));
		}
		if (getContextByKey(TupleDefinition.Context.DOCUMENT_ID) != null) {
			states.put(Message.State.DOCUMENT_ID.getCode(), getContextByKey(TupleDefinition.Context.DOCUMENT_ID));
		}
		if (getContextByKey(TupleDefinition.Context.PROFILE_IDS) != null) {
			states.put(Message.State.PROFILE_IDS.getCode(), getContextByKey(TupleDefinition.Context.PROFILE_IDS));
		}
		message.setStates(states);

		message.setType(getType());

		if (getResultCode() == TupleDefinition.Result.FAIL.getCode()) {
			message.setErrorMessage(StringUtils.join(getHistories(), " "));
		}

		return message;
	}

	@Override
	public String toString() {
		StringBuffer output = new StringBuffer("Result code ").append(getResultCode());

		if (!StringUtils.isEmpty(message)) {
			output.append(" - ").append(message);
		}
		if (!StringUtils.isEmpty(exceptionMessage)) {
			output.append(" - ").append(exceptionMessage);
		}
		if (!ArrayUtils.isEmpty(exceptionStackTrace)) {
			output.append("\n\t").append(StringUtils.join(exceptionStackTrace, "\n\t"));
		}

		return output.toString();
	}
}
