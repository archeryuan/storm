package com.sa.storm.sns.util;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.neovisionaries.i18n.CountryCode;
import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.SocialBakerCategoryRepository;
import com.sa.competitorhunter.dao.SocialBakerCompanyRepository;
import com.sa.competitorhunter.domain.Page;
import com.sa.competitorhunter.domain.SocialBakerCategory;
import com.sa.competitorhunter.domain.SocialBakerCompany;
import com.sa.storm.framework.App;
import com.sa.storm.sns.domain.SocialBakerLocalFans;

@Component
public class SocialBakerParser {

	private static final Logger log = LoggerFactory.getLogger(SocialBakerParser.class);

	private SocialBakerParser socialBakerParser;
	private static int MAX_PAGE = 30;
	private static String domain = "www.facebook.com";

	private SocialBakerCategoryRepository socialBakerCategoryRepository;
	private SocialBakerCompanyRepository socialBakerCompanyRepository;

	private SocialBakerParser() {
		socialBakerParser = SocialBakerParser.getInstance();
		socialBakerCategoryRepository = App.getInstance().getContext().getBean(SocialBakerCategoryRepository.class);
		socialBakerCompanyRepository = App.getInstance().getContext().getBean(SocialBakerCompanyRepository.class);

	}

	private static class Holder {
		public static SocialBakerParser instance;
		static {
			try {
				instance = new SocialBakerParser();
			} catch (Exception e) {
				log.error("Error in initialization", e);
			}
		}
	}

	public static SocialBakerParser getInstance() {
		return Holder.instance;
	}

	private static List<SocialBakerCompany> getCompanyList(String catUrl) throws IOException {
		List<SocialBakerCompany> companyList = new ArrayList<SocialBakerCompany>();
		for (int j = 1; j <= MAX_PAGE; j++) {
			String pageURL = catUrl + "page=" + j;
			Document doc = Jsoup.connect(pageURL).get();
			String cssQuery = ".item";
			Elements elements = doc.select(cssQuery);
			for (Element element : elements) {
				// log.debug(element.text());
				Elements subElements = element.children();
				if (subElements.hasAttr("href") && subElements.hasAttr("title")) {
					log.debug("href:" + subElements.attr("href"));
					SocialBakerCompany company = new SocialBakerCompany();
					company.setUrl("http://www.socialbakers.com/" + subElements.attr("href"));
					company.setCompanyName(subElements.attr("title"));
					try {
						company = getCompanyFrutherInfo(company);
					} catch (Exception ex) {
						ex.printStackTrace();
						// Skip
						continue;
					}
					companyList.add(company);
				}
			}
		}
		return companyList;
	}

	private static List<SocialBakerCompany> getLocalCompanyList(String catSuffix, String location, String abbrLoc) throws IOException {
		List<SocialBakerCompany> companyList = new ArrayList<SocialBakerCompany>();
		String URL = "http://www.socialbakers.com/statistics/facebook/pages/total/" + location + "/brands/" + catSuffix + "/";
		for (int j = 1; j <= MAX_PAGE; j++) {
			String pageURL = URL + "page-" + j;
			log.debug("Local URL:" + pageURL);
			Document doc = Jsoup.connect(pageURL).get();
			String cssQuery = ".item";
			Elements elements = doc.select(cssQuery);
			for (Element element : elements) {
				// log.debug(element.text());
				Elements subElements = element.children();
				if (subElements.hasAttr("href") && subElements.hasAttr("title")) {
					SocialBakerCompany company = new SocialBakerCompany();
					company.setUrl("http://www.socialbakers.com/" + subElements.attr("href"));
					company.setCompanyName(subElements.attr("title"));
					company.setLocation(abbrLoc);
					try {
						company = getCompanyFrutherInfo(company);
					} catch (Exception ex) {
						ex.printStackTrace();
						continue;
					}
					companyList.add(company);
				}
			}
			// body > div.page > div.content > div > section > section > div >
			// table > tbody > tr.replace-with-show-more > td > div > a

			if (doc.select(".more-center-link").isEmpty()) {
				break;
			}

		}
		return companyList;
	}

	private static SocialBakerCompany getCompanyFrutherInfo(SocialBakerCompany company) throws IOException {
		Document doc = Jsoup.connect(company.getUrl()).get();
		String cssQuery = ".account-detail img";
		Elements elements = doc.select(cssQuery);
		company.setIconURL(elements.get(0).attr("src"));
		cssQuery = ".account-detail ul li";
		elements = doc.select(cssQuery);
		for (Element element : elements) {
			Elements subElements = element.children();
			for (Element Lv2Element : subElements) {
				// log.debug(Lv2Element.toString());
				if (Lv2Element.hasAttr("href") && Lv2Element.hasAttr("target")) {
					String Lv2href = Lv2Element.attr("href");
					// log.debug("href:" + Lv2href);
					company.setFacebookURL(Lv2href);
					String id = "";
					if (NumberUtils.isNumber(Lv2href.substring(Lv2href.length() - 3, Lv2href.length() - 1))) {
						// Special case:
						// http://graph.facebook.com/pages/Novotel-Century-Hong-Kong/135060959631
						id = Lv2href.substring(Lv2href.lastIndexOf('/') + 1);
					} else {
						// General case: https://www.facebook.com/Accorhotels.hk
						id = Lv2href.substring(Lv2href.indexOf(domain) + domain.length() + 1);
					}
					JsonNode jNode = JsonUtil.getMapper().readTree(new URL("http://graph.facebook.com/" + id));
					company.setFacebookPageId(jNode.get("id").asText());
					log.debug("Facebook URL:" + company.getFacebookURL());
				}
			}
		}
		return company;
	}

	/**
	 * Convert the country name from Social Baker to international country code
	 * 
	 * @param sbCountryCode
	 * @return
	 * @throws Exception
	 */
	public String getCountryCode(final String sbCountryCode) throws Exception {
		if (sbCountryCode == null || StringUtils.isEmpty(sbCountryCode)) {
			return "";
		}
		String name = sbCountryCode.trim();
		String code = "";
		if ("Taiwan".equalsIgnoreCase(name)) {
			code = "TW";
		} else if ("South Korea".equalsIgnoreCase(name)) {
			code = "KR";
		} else if ("Vietnam".equalsIgnoreCase(name)) {
			code = "VN";
		} else if ("Laos".equalsIgnoreCase(name)) {
			code = "LA";
		} else if ("Palestinian Territory".equalsIgnoreCase(name)) {
			code = "PS";
		} else if (name.startsWith("Democratic Republic of the")) {
			// Congo
			code = "CD";
		} else if ("North Korea".equalsIgnoreCase(name)) {
			code = "KP";
		} else if ("Ivory Coast".equalsIgnoreCase(name)) {
			code = "CI";
		} else if ("British Virgin Islands".equalsIgnoreCase(name)) {
			code = "VG";
		} else if ("Reunion".equalsIgnoreCase(name)) {
			code = "RE";
		} else if ("Republic of the Congo".equalsIgnoreCase(name)) {
			code = "CG";
		} else {
			List<CountryCode> codeList = CountryCode.findByName(Pattern.compile(name) + ".*");
			if (codeList.size() == 0) {
				log.error("*****************Error:" + name);
			} else {
				code = codeList.get(0).getAlpha2();
			}
		}
		return code;
	}

	public List<SocialBakerLocalFans> getCompanyLocalFans(HtmlPage htmlPage, SocialBakerCompany company) throws Exception {

		List<SocialBakerLocalFans> localFanList = new ArrayList<SocialBakerLocalFans>();
		// log.debug("URL:" + company.getUrl());

		Document doc = Jsoup.parse(htmlPage.asXml());

		// body > div.page > div.content > div > section >
		// div.table-pie.table-pie-first > table > tbody > tr:nth-child(1) >
		// td.first > div > a
		// System.out.println(doc.text());
		String cssQuery = ".table-pie.table-pie-first > table > tbody";
		Elements elements = doc.select(cssQuery);
		// System.out.println("###################");
		// System.out.println(elements.html());
		// System.out.println("###################");
		if (elements.size() != 0) {
			Elements subElements = elements.get(0).children();
			for (Element element : subElements) {
				// Each country
				try {
					SocialBakerLocalFans fans = new SocialBakerLocalFans();
					// System.out.println("subElement:\n" + element.html());
					// Element ndLvlElement =
					// element.child(0).child(0).child(1);
					// System.out.println("*******************");
					// System.out.println(ndLvlElement.html());
					// System.out.println("*******************");
					// Element 2ndLvlElement = element.child(1);
					// Element abc = ndLvlElement.get(0).child(0);
					// String countryName =
					// ndLvlElement.getElementsByAttribute("href").get(0).text();
					String countryName = element.child(0).child(0).child(1).text();
					// System.out.println("Country:" + countryName);
					if (StringUtils.isEmpty(countryName)) {
						// Skip
						continue;
					}
					String localFansTxt = element.child(1).ownText();
					localFansTxt = StringEscapeUtils.escapeHtml(localFansTxt).replace("&nbsp;", "");
					// System.out.println("Local Fans:" + localFansTxt);
					fans.setCountry(countryName);
					fans.setNoOfLocalFan(Long.valueOf(localFansTxt));
					localFanList.add(fans);
				} catch (Exception ex) {
					continue;
				}
				// String localFansPercentTxt =
				// element.getElementsByAttributeValue("class",
				// "last").get(0).ownText();
				// localFansPercentTxt = localFansPercentTxt.replace(" %", "");
				// localFansPercentTxt =
				// StringEscapeUtils.escapeHtml(localFansPercentTxt).replace("&nbsp;",
				// "");
				// log.debug("countryName:" + countryName + ",localFans:" +
				// localFansTxt + ",%:" + localFansPercentTxt);
				// log.debug("****************");
			}
		}else {
			log.info(company.getCompanyName() + "(" + company.getFacebookPageId() + ") has no local fans");		
		}
		return localFanList;
	}

	public void saveSBCatAndCompmany() throws IOException {
		int noOfCat = SocialBakerCategory.CategoryName.values().length;
		log.debug("No of Social Baker Category:" + noOfCat);
		for (SocialBakerCategory.CategoryName SBCatname : SocialBakerCategory.CategoryName.values()) {
			// if (SBCatname.getCatId() < 15 || SBCatname.getCatId() > 17){
			// continue;
			// }
			log.debug(SBCatname.getName());
			SocialBakerCategory category = socialBakerCategoryRepository.findByCategoryName(SBCatname.getName());
			if (category != null) {
				category.setUrl(SBCatname.getUrl());
			} else {
				category = new SocialBakerCategory();
				category.setCategoryId(SBCatname.getCatId());
				category.setCategoryName(SBCatname.getName());
				category.setUrl(SBCatname.getUrl());
			}
			socialBakerCategoryRepository.saveAndFlush(category);
			// Get Company List
			List<SocialBakerCompany> companyList = SocialBakerParser.getCompanyList(category.getUrl());
			for (SocialBakerCompany company : companyList) {
				company.setCategoryId(category);
				company.setCreateDate(new Date());
				log.debug("PageID:" + category.getCategoryName() + " - " + company.getFacebookPageId());
				socialBakerCompanyRepository.saveAndFlush(company);
				// log.debug("****************************************");
				// log.debug(company.getCompanyName() + ":" + company.getUrl());
				// log.debug(company.getIconURL());
				// log.debug(company.getFacebookURL());
			}
		}
	}

	/**
	 * 
	 * @param location
	 * @param abbrLoc
	 * @param startCatId
	 *            Begin of SB Category ID
	 * @param skipCatId
	 *            Skip list
	 * @throws IOException
	 */
	public void saveSBLocalCompany(Page.Location location, int startCatId, int[] skipCatId) throws IOException {
		String prefix = "http://www.socialbakers.com/statistics/facebook/pages/total/brands/";
		List<SocialBakerCategory> sbCatList = socialBakerCategoryRepository.findAll();
		log.debug("No of Social Baker Category:" + sbCatList.size());
		for (SocialBakerCategory sbCat : sbCatList) {

			if (sbCat.getCategoryId() < startCatId || Arrays.asList(skipCatId).contains(sbCat.getCategoryId())) {
				continue;
			}
			log.debug("SB Category:" + sbCat.getCategoryName());
			String catSuffix = sbCat.getUrl().substring(prefix.length(), sbCat.getUrl().lastIndexOf('?'));
			List<SocialBakerCompany> companyList = SocialBakerParser.getLocalCompanyList(catSuffix, location.getSocialBakerStr(), location.getDbStr());
			for (SocialBakerCompany company : companyList) {
				company.setCategoryId(sbCat);
				company.setCreateDate(new Date());
				log.debug("PageID:" + sbCat.getCategoryName() + " - " + company.getCompanyName());
				socialBakerCompanyRepository.saveAndFlush(company);
			}
		}
	}

	public void updateFacebookId() throws IOException {
		List<SocialBakerCompany> companyList = socialBakerCompanyRepository.findAll();
		for (SocialBakerCompany sbCompany : companyList) {
			String pageId = sbCompany.getFacebookPageId();
			if (StringUtils.isAlpha(pageId)) {
				System.out.println(pageId);
				// JsonNode jNode = JsonUtil.getMapper().readTree(new
				// URL("http://graph.facebook.com/" + pageId));
				// sbCompany.setFacebookPageId(jNode.get("id").asText());
				// socialBakerCompanyRepository.saveAndFlush(sbCompany);
			}
		}
	}

	public static void main(String[] args) throws IOException {
		SocialBakerParser parser = new SocialBakerParser();
		parser.updateFacebookId();

	}

}
