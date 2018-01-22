package com.sa.storm.spider.domain;

import java.io.Serializable;

public class WebPageResponse implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 9181198659880469294L;
	private int httpStatusCode;
	private String htmlSourceCode;
	private String url;
	private String fromUrl;
	private String cookies;
	private int execReturnCode;
	private String commandline;
	private String util;

	public int getHttpStatusCode() {
		return httpStatusCode;
	}

	public void setHttpStatusCode(int httpStatusCode) {
		this.httpStatusCode = httpStatusCode;
	}

	public String getHtmlSourceCode() {
		return htmlSourceCode;
	}

	public void setHtmlSourceCode(String htmlSourceCode) {
		this.htmlSourceCode = htmlSourceCode;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getCookies() {
		return cookies;
	}

	public void setCookies(String cookies) {
		this.cookies = cookies;
	}

	public String getFromUrl() {
		return fromUrl;
	}

	public void setFromUrl(String fromurl) {
		this.fromUrl = fromurl;
	}

	public int getExecReturnCode() {
		return execReturnCode;
	}

	public void setExecReturnCode(int execReturnCode) {
		this.execReturnCode = execReturnCode;
	}

	public String getCommandline() {
		return commandline;
	}

	public void setCommandline(String commandline) {
		this.commandline = commandline;
	}

	public String getUtil() {
		return util;
	}

	public void setUtil(String util) {
		this.util = util;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((commandline == null) ? 0 : commandline.hashCode());
		result = prime * result + ((cookies == null) ? 0 : cookies.hashCode());
		result = prime * result + execReturnCode;
		result = prime * result + ((fromUrl == null) ? 0 : fromUrl.hashCode());
		result = prime * result + ((htmlSourceCode == null) ? 0 : htmlSourceCode.hashCode());
		result = prime * result + httpStatusCode;
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		result = prime * result + ((util == null) ? 0 : util.hashCode());
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
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		WebPageResponse other = (WebPageResponse) obj;
		if (commandline == null) {
			if (other.commandline != null)
				return false;
		} else if (!commandline.equals(other.commandline))
			return false;
		if (cookies == null) {
			if (other.cookies != null)
				return false;
		} else if (!cookies.equals(other.cookies))
			return false;
		if (execReturnCode != other.execReturnCode)
			return false;
		if (fromUrl == null) {
			if (other.fromUrl != null)
				return false;
		} else if (!fromUrl.equals(other.fromUrl))
			return false;
		if (htmlSourceCode == null) {
			if (other.htmlSourceCode != null)
				return false;
		} else if (!htmlSourceCode.equals(other.htmlSourceCode))
			return false;
		if (httpStatusCode != other.httpStatusCode)
			return false;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		if (util == null) {
			if (other.util != null)
				return false;
		} else if (!util.equals(other.util))
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
		StringBuilder builder = new StringBuilder();
		builder.append("WebPageResponse [httpStatusCode=");
		builder.append(httpStatusCode);
		builder.append(", ");
		if (htmlSourceCode != null) {
			builder.append("htmlSourceCode=");
			builder.append(htmlSourceCode);
			builder.append(", ");
		}
		if (url != null) {
			builder.append("url=");
			builder.append(url);
			builder.append(", ");
		}
		if (fromUrl != null) {
			builder.append("fromUrl=");
			builder.append(fromUrl);
			builder.append(", ");
		}
		if (cookies != null) {
			builder.append("cookies=");
			builder.append(cookies);
			builder.append(", ");
		}
		builder.append("execReturnCode=");
		builder.append(execReturnCode);
		builder.append(", ");
		if (commandline != null) {
			builder.append("commandline=");
			builder.append(commandline);
			builder.append(", ");
		}
		if (util != null) {
			builder.append("util=");
			builder.append(util);
		}
		builder.append("]");
		return builder.toString();
	}

}
