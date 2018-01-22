package com.sa.storm.spider.domain;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataResult implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4648319955285777162L;

	private static final Logger log = LoggerFactory.getLogger(DataResult.class);

	// 记录content的个数，以便生成相应个数的 images 集合
	private int size = 0;
	private HashMap<String, List<Object>> map;
	private WebPageResponse pageResponse;
	private List<Set<String>> imgUrls;

	public DataResult() {
	}

	public DataResult(WebPageResponse wpr) {
		this.map = new HashMap<String, List<Object>>();
		this.imgUrls = new ArrayList<Set<String>>();
		this.pageResponse = wpr;
	}

	@JsonProperty("map")
	public HashMap<String, List<Object>> getMap() {
		return map;
	}

	@JsonProperty("map")
	public void setMap(HashMap<String, List<Object>> map) {
		this.map = map;
	}

	@JsonIgnore
	public WebPageResponse getWebPageResponse() {
		return pageResponse;
	}

	@JsonIgnore
	public void setWebPageResponse(WebPageResponse wpr) {
		this.pageResponse = wpr;
	}

	@JsonProperty("imgUrls")
	public List<Set<String>> getImgUrls() {
		return imgUrls;
	}

	@JsonProperty("imgUrls")
	public void setImgUrls(List<Set<String>> imgurls) {
		this.imgUrls = imgurls;
	}

	@JsonProperty("size")
	public int getSize() {
		return size;
	}

	@JsonProperty("size")
	public void setSize(int size) {
		this.size = size;
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
		result = prime * result + ((imgUrls == null) ? 0 : imgUrls.hashCode());
		result = prime * result + ((map == null) ? 0 : map.hashCode());
		result = prime * result + ((pageResponse == null) ? 0 : pageResponse.hashCode());
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
		DataResult other = (DataResult) obj;
		if (imgUrls == null) {
			if (other.imgUrls != null)
				return false;
		} else if (!imgUrls.equals(other.imgUrls))
			return false;
		if (map == null) {
			if (other.map != null)
				return false;
		} else if (!map.equals(other.map))
			return false;
		if (pageResponse == null) {
			if (other.pageResponse != null)
				return false;
		} else if (!pageResponse.equals(other.pageResponse))
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
		builder.append("DataResult [size=");
		builder.append(size);
		builder.append(", ");
		if (map != null) {
			builder.append("map=");
			builder.append(map);
			builder.append(", ");
		}
		if (pageResponse != null) {
			builder.append("pageResponse=");
			builder.append(pageResponse);
			builder.append(", ");
		}
		if (imgUrls != null) {
			builder.append("imgUrls=");
			builder.append(imgUrls);
		}
		builder.append("]");
		return builder.toString();
	}

	// public SpiderException parserData(SpiderRule rule) {
	// String name = rule.getS_name();
	// String selector = rule.getS_selector();
	// String attr = rule.getS_attr();
	// String format = rule.getS_format();
	// String hbase_field = rule.getHbase_field();
	// // int index = rule.getS_index();
	// // int priority = rule.getS_priority();
	// String reg = rule.getS_reg();
	// boolean isTime = rule.isTime();
	//
	// SpiderException spiderException = null;
	//
	// if (pageResponse.getHtmlSourceCode() == null) {
	// log.warn("   pageResponse.getHtmlSourceCode() == null   ");
	// spiderException = new SpiderException(null, ExceptionDefinition.SOURCE_CODE_NOT_OK, pageResponse.getUrl(),
	// "pageResponse.getHtmlSourceCode() == null");
	// } else if (selector == null) {
	// log.warn(name + " selector value is null");
	// spiderException = new SpiderException(null, ExceptionDefinition.SELECTOR_NOT_OK, pageResponse.getUrl(), "selector value is null");
	// } else {
	// Document doc = Jsoup.parse(pageResponse.getHtmlSourceCode());
	// Elements elements = doc.select(selector);
	//
	// log.warn(hbase_field+" ---- "+name+" ---- "+selector+" ---- elements.size() ---> "+elements.size()+" ---- "+pageResponse.getUrl());
	// // log.warn("-----elements.size()---->"+elements.size());
	//
	// // 为了能配置多个selector 解析doc , 这里 key 是 selector name, value 是得到的list。使用hashmap 保证同样的selector 对应同一个list
	// List<Object> list = map.get(hbase_field) == null ? new ArrayList<Object>() : map.get(hbase_field);
	// if (elements.size() > 0) {
	// for (int i = 0; i < elements.size(); ++i) {
	// Element elem = elements.get(i);
	//
	// // log.warn("----name-----"+name+"------"+selector+"-----selector---->"+e.outerHtml());
	//
	// String text = null;
	//
	// if (attr == null || "".equals(attr.trim())) {
	// text = elem.text();
	// } else {
	// if (elem.attr(attr).equals("")) {
	// spiderException = new SpiderException(null, ExceptionDefinition.USELESS_SELECTOR, pageResponse.getUrl(),
	// name+": "+selector+"["+attr+"] is useless, it found nothing!");
	// } else {
	// text = elem.attr(attr);
	// }
	// }
	//
	// if (reg != null && !"".equals(reg.trim())) {
	// text = replaceReg(text, reg);
	// }
	//
	// if ("href".equals(attr)) {
	// text = HtmlParseUtil.getUrlFullPath(pageResponse.getUrl(), text);
	// }
	//
	// if (isTime) {
	// Date date = null;
	//
	// try {
	// date = DateUtil.getDate(text, format);
	// } catch (ParseException e1) {
	// // xx 时间以前，标准时间的dateformate 无法解析
	// log.warn("解析的时间异常：>>>> " + text + " <<<<");
	// date = DateUtil.specialCase(text);
	// }
	//
	// list.add(i, date);
	// } else {
	// list.add(i, text);
	// }
	//
	// if ("content".equals(hbase_field)) {
	// Set<String> urls = HtmlParseUtil.getImgUrls(pageResponse.getUrl(), elem.outerHtml());
	//
	// imgUrls.add(i, urls);
	//
	// size = elements.size();
	// }
	// }
	//
	// // log.warn(list.toString());
	// } else {
	// if ("link".equals(hbase_field) && "#SELF".equals(selector)) {
	// list.add(pageResponse.getUrl());
	// } else {
	// spiderException = new SpiderException(null, ExceptionDefinition.USELESS_SELECTOR, pageResponse.getUrl(),
	// name+": "+selector+" is useless, it found nothing!");
	// }
	// }
	//
	// map.put(hbase_field, list);
	// }
	//
	// return spiderException;
	// }

}