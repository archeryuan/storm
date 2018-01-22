package com.sa.storm.spider.tracer;

public class SeedTupleTracer extends TupleTracer {

	/**
	 * 
	 */
	private static final long serialVersionUID = -481741626771353595L;

	private String url;

	private String fromUrl;

	private int httpStatusCode;

	private int execode;

	private String commandline;

	private String htmlSourceCode;

	private String cookies;

	private String exceptionMessage;

	/**
	 * 
	 */
	public SeedTupleTracer() {
	}

	// @Deprecated
	// public void putWebPageResponse(WebPageResponse wr) {
	// this.commandline = wr.getCommandline();
	// this.cookies = wr.getCookies();
	// this.execode = wr.getExecReturnCode();
	// this.url = wr.getUrl();
	// this.fromUrl = wr.getFromUrl();
	// this.htmlSourceCode = wr.getHtmlSourceCode();
	// this.httpStatusCode = wr.getHttpStatusCode();
	// }

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getFromUrl() {
		return fromUrl;
	}

	public void setFromUrl(String fromUrl) {
		this.fromUrl = fromUrl;
	}

	public int getHttpStatusCode() {
		return httpStatusCode;
	}

	public void setHttpStatusCode(int httpstatuscode) {
		this.httpStatusCode = httpstatuscode;
	}

	public int getExecode() {
		return execode;
	}

	public void setExecode(int execode) {
		this.execode = execode;
	}

	public String getCommandline() {
		return commandline;
	}

	public void setCommandline(String commandline) {
		this.commandline = commandline;
	}

	public String getHtmlSourceCode() {
		return htmlSourceCode;
	}

	public void setHtmlSourceCode(String htmlSourceCode) {
		this.htmlSourceCode = htmlSourceCode;
	}

	public String getCookies() {
		return cookies;
	}

	public void setCookies(String cookies) {
		this.cookies = cookies;
	}

	public String getExceptionMessage() {
		return exceptionMessage;
	}

	public void setExceptionMessage(String exceptionMessage) {
		this.exceptionMessage = exceptionMessage;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SeedTupleTracer [");
		if (url != null) {
			builder.append("url=");
			builder.append(url);
			builder.append(", ");
		}
		if (fromUrl != null) {
			builder.append("fromurl=");
			builder.append(fromUrl);
			builder.append(", ");
		}
		builder.append("httpstatuscode=");
		builder.append(httpStatusCode);
		builder.append(", execode=");
		builder.append(execode);
		builder.append(", ");
		if (commandline != null) {
			builder.append("commandline=");
			builder.append(commandline);
			builder.append(", ");
		}
		if (htmlSourceCode != null) {
			builder.append("htmlSourceCode=");
			builder.append(htmlSourceCode);
			builder.append(", ");
		}
		if (cookies != null) {
			builder.append("cookies=");
			builder.append(cookies);
			builder.append(", ");
		}
		if (exceptionMessage != null) {
			builder.append("exceptionMessage=");
			builder.append(exceptionMessage);
		}
		builder.append("]");
		return builder.toString();
	}

}
