package social.hunt.analysis.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.sa.storm.framework.App;

import social.hunt.data.domain.DomainCategory;
import social.hunt.data.service.DomainCategoryService;

public class TestDomainExtract {

	private static final Logger log = LoggerFactory.getLogger(TestDomainExtract.class);

	private DomainCategoryService service;

	protected ApplicationContext appContext;

	Map<String, DomainCategory> dataMap;

	private List<DomainCategory> domainList;
	private static final String HTTP = "http://";
	private static final String HTTPS = "https://";

	@Test
	public void test() {
		dataMap = loadTable();

		String url = "http://14621763.blog.hexun.com.tw/103140066_d.html";

		String domain = "";
		String tmp = "";
		if (StringUtils.startsWith(url, HTTPS)) {
			tmp = StringUtils.substringAfter(url, HTTPS);
		} else if (StringUtils.startsWith(url, HTTP)) {
			tmp = StringUtils.substringAfter(url, HTTP);
		}

		tmp = StringUtils.substringBefore(tmp, "/");
		domain = StringUtils.substringBefore(tmp, ":");
		log.info("Remove HTTP : {}", domain);

		domain = domainExtact(domain);
		if (isExactMatch(domain)) {
			DomainCategory data = dataMap.get(domain);
			log.info("extracted object: {}", data.toString());
		} else {
			log.info("Not EXACT match");
		}
	}

	public String domainExtact(String httpRemoved) {
		String[] elements = StringUtils.split(httpRemoved, ".");
		String domain = "";
		if (elements.length > 1) {
			for (int i = 0; i < elements.length; i++) {
				String element = elements[i] + "." + elements[i + 1];
				log.info("element: {}", element);
				if (isSubstringMatch(element)) {
					if (i + 2 < elements.length) {
						String temp = element + "." + elements[i + 2];
						log.info("element: {}", temp);
						if (isSubstringMatch(temp)) {
							domain = temp;
							log.info("Final extracted domain: {}", domain);
							return domain;
						} else {
							domain = element;
							log.info("Final extracted domain: {}", domain);
							return domain;
						}
					} else {
						domain = element;
						log.info("Final extracted domain: {}", domain);
						return domain;
					}
				}
				log.info("Not matched");
			}
		}
		return httpRemoved;
	}

	public boolean isExactMatch(String element) {
		for (Map.Entry<String, DomainCategory> entry : dataMap.entrySet()) {
			if (StringUtils.equalsIgnoreCase(entry.getKey(), element))
				return true;
		}
		return false;
	}

	public boolean isSubstringMatch(String element) {
		for (Map.Entry<String, DomainCategory> entry : dataMap.entrySet()) {
			if (StringUtils.contains(entry.getKey(), element))
				return true;
		}
		return false;
	}

	private Map<String, DomainCategory> loadTable() {
		dataMap = new HashMap<String, DomainCategory>();
		appContext = App.getInstance().getContext();
		service = appContext.getBean(DomainCategoryService.class);

		for (DomainCategory domainCategory : domainList) {
			dataMap.put(domainCategory.getDomain(), domainCategory);
		}
		log.info("Size of domain map: {}", dataMap.size());

		return dataMap;
	}

	private void checkDomainElementsNo(Map<String, DomainCategory> dataMap) {

		int count = 0;
		for (Map.Entry<String, DomainCategory> entry : dataMap.entrySet()) {
			String domainStr = entry.getKey();
			String[] arrayStrings = StringUtils.split(domainStr, ".");
			if (arrayStrings.length > 2) {
				count++;
			}
		}
		log.info("No of arrays > 2: {}", count);
	}

}
