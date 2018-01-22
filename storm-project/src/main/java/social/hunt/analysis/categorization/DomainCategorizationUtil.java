package social.hunt.analysis.categorization;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.propertyeditors.StringTrimmerEditor;
import org.springframework.context.ApplicationContext;

import social.hunt.data.domain.DomainCategory;
import social.hunt.data.service.DomainCategoryService;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sa.common.json.JsonUtil;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.framework.App;

public class DomainCategorizationUtil {
	private static final Logger log = LoggerFactory.getLogger(DomainCategorizationUtil.class);

	private final RedisUtil redisUtil;
	private ApplicationContext appContext;
	private DomainCategoryService domainService;

	private static final String REGION_FIELD = "region";
	private static final String SOURCE_TYPE_FIELD = "sourceType";
	private static final String LEVEL_FIELD = "level";
	private static final String DOMAIN_FIELD = "domain";

	public static void main(String[] args) throws Exception {
		DomainCategory obj = new DomainCategory();
		obj.setDomain("yahoo.com");
		obj.setLevel((short) 1);
		obj.setRegion((short) 1);
		obj.setSourceType((short) 99);

		Map<String, String> domainMap = new HashMap<String, String>();
		domainMap.put("yahoo.com", JsonUtil.getMapper().writeValueAsString(obj));

		RedisUtil.getInstance().hmset(RedisDefinition.StormDef.DOMAIN_CATEGORY_HASH, domainMap);

		String jsonStr = RedisUtil.getInstance().hget(RedisDefinition.StormDef.DOMAIN_CATEGORY_HASH, "adsf.com");
		log.info("res: {}", jsonStr);
	}

	public DomainCategorizationUtil() throws Exception {
		redisUtil = RedisUtil.getInstance();

		if (!redisUtil.exists(RedisDefinition.StormDef.DOMAIN_CATEGORY_HASH)) {
			writeCatToRedis();
		}
	}

	public void writeCatToRedis() throws JsonProcessingException {
		log.info("Writing domain cat from sql to redis...");
		appContext = App.getInstance().getContext();
		domainService = appContext.getBean(DomainCategoryService.class);

		Map<String, DomainCategory> domainMap = domainService.getDataMap();
		Map<String, String> hash = new HashMap<String, String>();
		for (Entry<String, DomainCategory> entry : domainMap.entrySet()) {
			hash.put(entry.getKey(), JsonUtil.getMapper().writeValueAsString(entry.getValue()));
		}
		redisUtil.hmset(RedisDefinition.StormDef.DOMAIN_CATEGORY_HASH, hash);
		log.info("Finished writing domain cat to redis");
	}

	public DomainCategory getDomainCat(String domain) throws JSONException {
		String jsonStr = redisUtil.hget(RedisDefinition.StormDef.DOMAIN_CATEGORY_HASH, domain);
		if (jsonStr != null) {
			return jsonToDomainCat(jsonStr);
		}
		return null;
	}

	public static DomainCategory jsonToDomainCat(String jsonStr) throws JSONException {
		JSONObject json = new JSONObject(jsonStr);

		Short region = (short) json.optInt(REGION_FIELD);
		Short level = (short) json.optInt(LEVEL_FIELD);
		Short sourceType = (short) json.optInt(SOURCE_TYPE_FIELD);
		String domain = json.optString(DOMAIN_FIELD);

		log.info("domain: {} region: {} level: {} sourceType : {}", new Object[] { domain, region, level, sourceType });

		DomainCategory category = new DomainCategory();
		category.setRegion(region);
		category.setLevel(level);
		category.setSourceType(sourceType);
		category.setDomain(domain);

		return category;
	}

}
