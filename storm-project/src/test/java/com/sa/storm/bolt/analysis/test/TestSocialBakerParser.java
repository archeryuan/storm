package com.sa.storm.bolt.analysis.test;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.fasterxml.jackson.databind.JsonNode;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.sa.common.dao.config.CommonDaoConfig;
import com.sa.common.json.JsonUtil;
import com.sa.competitorhunter.dao.LocalFansRepository;
import com.sa.competitorhunter.dao.PageRepository;
import com.sa.competitorhunter.dao.SocialBakerCategoryRepository;
import com.sa.competitorhunter.dao.SocialBakerCompanyRepository;
import com.sa.competitorhunter.domain.LocalFans;
import com.sa.competitorhunter.domain.Page;
import com.sa.competitorhunter.domain.SocialBakerCategory;
import com.sa.competitorhunter.domain.SocialBakerCompany;
import com.sa.competitorhunter.token.FacebookAccessToken;
import com.sa.storm.sns.domain.SocialBakerLocalFans;
import com.sa.storm.sns.util.SocialBakerParser;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { CommonDaoConfig.class })
public class TestSocialBakerParser {

	private static SocialBakerParser socialBakerParser;

	@Autowired
	private SocialBakerCompanyRepository socialBakerCompanyRepository;

	@Autowired
	private SocialBakerCategoryRepository socialBakerCategoryRepository;

	@Autowired
	private LocalFansRepository localFansRepository;

	@Autowired
	private PageRepository pageRepository;

	private String mobileToken = "CAAAAAYsX7TsBAFUVG8N6o63oBDFuWYiZCa6d8dijv17tpKcCrNrZAtxHnCMzZAHt9Xl7DMZAnEfvhk7XYNW6A2nEZCdE69lcOk7htWBOM41g4u8c1fMnQT66Y3x5Y6DONZCUn6YFDM9KvaSPTq5OgaxTiG3KnBvJIJ1GJo70pkXpDfnuRHVhkrIkAxa8h0ba92LWayJUlv5Mp2j2ccclDjfHdcqAyAvud0wbWkxDHcdQ1vvAdVsvKo";

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		socialBakerParser = SocialBakerParser.getInstance();
	}

	// @Test
	public void updateFaceBookId() throws Exception {
		List<SocialBakerCompany> companyList = socialBakerCompanyRepository.findAll();
		for (SocialBakerCompany sbCompany : companyList) {
			String pageId = sbCompany.getFacebookPageId();
			if (StringUtils.isAlpha(pageId)) {
				try {
					JsonNode jNode = JsonUtil.getMapper().readTree(new URL("https://graph.facebook.com/v2.2/" + pageId + "?access_token=" + mobileToken));
					sbCompany.setFacebookPageId(jNode.get("id").asText());
					socialBakerCompanyRepository.saveAndFlush(sbCompany);
				} catch (Exception ex) {
					System.out.println(pageId);
					socialBakerCompanyRepository.delete(sbCompany);
				}
			}
		}
	}

//	@Test
	public void updateSBURL() throws Exception {
		List<SocialBakerCompany> companyList = socialBakerCompanyRepository.findAll();
		for (SocialBakerCompany sbCompany : companyList) {
			String pageId = sbCompany.getFacebookPageId();
			if (StringUtils.isAlpha(pageId)) {
				try {
					JsonNode jNode = JsonUtil.getMapper().readTree(new URL("https://graph.facebook.com/v2.2/" + pageId + "?access_token=" + mobileToken));
					sbCompany.setFacebookPageId(jNode.get("id").asText());
					socialBakerCompanyRepository.saveAndFlush(sbCompany);
				} catch (Exception ex) {
					System.out.println(pageId);
					socialBakerCompanyRepository.delete(sbCompany);
				}
			}
		}
	}

	
	//@Test
	public void getLocalFans() throws Exception {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		String statisticsDate = df.format(new Date());
		// Get Log on Token
		FacebookAccessToken access = new FacebookAccessToken("639439551863", "9439551863");
		access.login();
		access.getSBAuthorization();
		// List<SocialBakerCompany> companyList =
		// socialBakerCompanyRepository.findByLocation(Page.Location.HONG_KONG.getDbStr());
		// for (SocialBakerCompany sbCompany:companyList){
		List<Page> pageList = pageRepository.findByLocation(Page.Location.HONG_KONG.getDbStr());
		// List<Page> pageList = pageRepository.findAll();
		for (Page page : pageList) {
			if (page.getId() < 16495){
				continue;
			}
			SocialBakerCompany sbCompany = socialBakerCompanyRepository.findByFacebookPageId(page.getPageId());
			System.out.println("Social Baker URL:" + sbCompany.getUrl());
			List<SocialBakerLocalFans> sbFanList = new ArrayList<SocialBakerLocalFans> () ;
			try{
				HtmlPage htmlPage = access.getHTMLPage(sbCompany.getUrl());
				sbFanList = socialBakerParser.getCompanyLocalFans(htmlPage, sbCompany);
			}catch (Exception ex){
				ex.printStackTrace();
				continue;
			}
			for (SocialBakerLocalFans sbLocalFan : sbFanList) {
				try {

					// System.out.println("Country (" + sbLocalFan.getCountry()
					// + "):" + sbLocalFan.getNoOfLocalFan());
					String location = socialBakerParser.getCountryCode(sbLocalFan.getCountry().trim());
					if (!StringUtils.isEmpty(location)) {
						List<LocalFans> localFansList = localFansRepository.findByMediaTypeIdAndPageIdAndLocation(page.getMediaTypeId(),
								Long.valueOf(page.getPageId()), location);
						LocalFans localFans;
						if (localFansList == null || localFansList.size() == 0) {
							localFans = new LocalFans();
							localFans.setMediaTypeId(page.getMediaTypeId());
							localFans.setPageId(Long.valueOf(page.getPageId()));
							localFans.setLocation(location);
						} else {
							localFans = localFansList.get(0);
						}
						localFans.setLocalFans(sbLocalFan.getNoOfLocalFan());
						localFans.setStatisticsDate(statisticsDate);
						localFans.setUpdateDate(new Date());
						localFansRepository.saveAndFlush(localFans);
					}
				} catch (Exception ex) {
					ex.printStackTrace();
					continue;
				}
			}
		}
	}

	// @Test
	public void copyLocalCompnayfromSBToCH() throws IOException {
		List<SocialBakerCompany> companyList = socialBakerCompanyRepository.findByLocation(Page.Location.HONG_KONG.getDbStr());
		int count = 0;
		for (SocialBakerCompany sb : companyList) {
			System.out.println("PageID:" + sb.getFacebookPageId());
			Page page = pageRepository.findByPageId(sb.getFacebookPageId());
			if (page == null) {
				page = new Page();
				SocialBakerCategory cat = socialBakerCategoryRepository.findByCategoryId(sb.getCategoryId().getCategoryId());
				page.setCategory(cat.getCategoryName());
				page.setCategoryId(cat.getCategoryId());
				page.setCoverImageUrl(sb.getIconURL());
				page.setCreateDate(new Date());
				if (cat.getCategoryId() == 10) {
					page.setEnabled(true);
					page.setCrawlEnabled(true);
				} else {
					page.setEnabled(false);
					page.setCrawlEnabled(false);
				}
				page.setIsCompetitor(0);
				page.setLocation(Page.Location.HONG_KONG.getDbStr());
				page.setLogoUrl(sb.getIconURL());
				page.setMediaTypeId(8);
				page.setOwner(null);
				page.setPageId(sb.getFacebookPageId());
				page.setPageLink(sb.getFacebookURL());
				page.setPageName(sb.getCompanyName());
				page.setShareImageUrl(null);
				pageRepository.saveAndFlush(page);
				count++;
			} else {
				System.out.println(sb.getFacebookPageId() + "already exists");
			}
		}
		System.out.println("Added Page:" + count);
	}

	// @Test
	public void getSBCatAndCompmany() throws Exception {

		socialBakerParser.saveSBCatAndCompmany();
	}

	
	/**
	 * Get local company.
	 * 
	 */
	@Test
	public void getSBLocalCompany() throws IOException {

		int startCatId = 15;
		int[] skipCatId = { 14 };

		//socialBakerParser.saveSBLocalCompany(Page.Location.HONG_KONG, startCatId, skipCatId);
		
		socialBakerParser.saveSBLocalCompany(Page.Location.TAIWAN, 25, skipCatId);
		
		System.out.println("--------------getSBLocalCompany done---------------");
	}

}
