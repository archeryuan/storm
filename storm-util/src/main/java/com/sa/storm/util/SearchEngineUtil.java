package com.sa.storm.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

public class SearchEngineUtil {
	public enum ComplexSearchSite {
		/**
		 * Baidu
		 */
		BAIDU("baidu", "", "http://www.baidu.com/s?rn=100&wd="),

		/**
		 * Yahoo HK
		 */
		YAHOO_HK("yahoo_hk", "", "http://hk.search.yahoo.com/search?n=40&ei=UTF-8&va_vt=any&vo_vt=any&ve_vt=any&vp_vt=any&vf=all&fl=0&p="),

		/*
		 * Yahoo TW
		 */
		YAHOO_TW("yahoo_tw", "", "http://tw.search.yahoo.com/search?n=100&ei=UTF-8&va_vt=any&vo_vt=any&ve_vt=any&vp_vt=any&vf=all&fl=0&p=");

		private String type;
		private String prefix;
		private String url;

		/**
		 * @param type
		 * @param prefix
		 * @param url
		 */
		private ComplexSearchSite(String type, String prefix, String url) {
			this.type = type;
			this.prefix = prefix;
			this.url = url;
		}

		/**
		 * @return the type
		 */
		public String getType() {
			return type;
		}

		/**
		 * @param type
		 *            the type to set
		 */
		public void setType(String type) {
			this.type = type;
		}

		/**
		 * @return the prefix
		 */
		public String getPrefix() {
			return prefix;
		}

		/**
		 * @param prefix
		 *            the prefix to set
		 */
		public void setPrefix(String prefix) {
			this.prefix = prefix;
		}

		/**
		 * @return the url
		 */
		public String getUrl() {
			return url;
		}

		/**
		 * @param url
		 *            the url to set
		 */
		public void setUrl(String url) {
			this.url = url;
		}

		public static ComplexSearchSite getSite(String type) {
			if (type == null)
				return null;

			for (ComplexSearchSite site : ComplexSearchSite.values()) {
				if (site.getType().equals(type))
					return site;
			}

			return null;
		}
	}

	public static String executeSearch(String type, String keywordsL1, String keywordsL2, List<String> siteList) {
		List<String> keywordsString = new ArrayList<String>();
		if (ComplexSearchSite.BAIDU.getType().equals(type)) {

			String siteString = "site:(" + StringUtils.join(siteList, " | ") + ")";
			String l1 = keywordsL1;
			String l2 = keywordsL2;
			String[] l1s = l1.split(",");
			for (String l1Kw : l1s) {
				String kw1Tmp = "\"" + l1Kw + "\"";
				if (l2 != null && !"".equals(l2)) {
					String[] l2s = l2.split(",");
					for (String l2Kw : l2s) {
						String kw2Tmp = "\"" + l2Kw + "\"";
						keywordsString.add(kw1Tmp + " " + kw2Tmp);
					}
				} else {
					keywordsString.add(kw1Tmp);
				}

			}
			String finalString = "(" + StringUtils.join(keywordsString, ") | (") + ")";

			return ComplexSearchSite.BAIDU.getUrl() + siteString + " " + finalString;
		} else if (ComplexSearchSite.YAHOO_TW.getType().equals(type)) {
			String siteString = "&vs=" + StringUtils.join(siteList, "%2C");
			String l1 = keywordsL1;
			String l2 = keywordsL2;
			String[] l1s = l1.split(",");
			for (String l1Kw : l1s) {
				String kw1Tmp = "\"" + l1Kw + "\"";
				if (l2 != null && !"".equals(l2)) {
					String[] l2s = l2.split(",");
					for (String l2Kw : l2s) {
						String kw2Tmp = "\"" + l2Kw + "\"";
						keywordsString.add(kw1Tmp + " " + kw2Tmp);
					}
				} else {
					keywordsString.add(kw1Tmp);
				}
			}
			String finalString = StringUtils.join(keywordsString, " OR ");

			return ComplexSearchSite.YAHOO_TW.getUrl() + finalString + siteString;
		} else if (ComplexSearchSite.YAHOO_HK.getType().equals(type)) {
			String siteString = "&vs=" + StringUtils.join(siteList, "%2C");
			String l1 = keywordsL1;
			String l2 = keywordsL2;
			String[] l1s = l1.split(",");
			for (String l1Kw : l1s) {
				String kw1Tmp = "\"" + l1Kw + "\"";
				if (l2 != null && !"".equals(l2)) {
					String[] l2s = l2.split(",");
					for (String l2Kw : l2s) {
						String kw2Tmp = "\"" + l2Kw + "\"";
						keywordsString.add(kw1Tmp + " " + kw2Tmp);
					}
				} else {
					keywordsString.add(kw1Tmp);
				}
			}
			String finalString = StringUtils.join(keywordsString, " OR ");

			return ComplexSearchSite.YAHOO_HK.getUrl() + finalString + siteString;
		} else {
			return null;
		}
	}
}
