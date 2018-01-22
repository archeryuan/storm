/**
 * 
 */
package social.hunt.analysis.sentiment;

import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.sa.common.config.CommonConfig;
import com.sa.redis.definition.RedisDefinition.StormDef;
import com.sa.redis.util.RedisUtil;
import com.semantria.CallbackHandler;
import com.semantria.Session;
import com.semantria.mapping.configuration.Configuration;
import com.semantria.mapping.output.Subscription;
import com.semantria.serializer.JsonSerializer;

/**
 * @author Jason
 *
 */
public class SemantriaService {

	private static final Logger log = LoggerFactory.getLogger(SemantriaService.class);

	// Account Keys
	private final static String KEY = "92eb3604-279b-4d2b-ae16-fc32fa39dca5";
	private final static String SECRET = "daffea1e-b55b-4488-a94b-7cb5fb6ce6e8";

	// Config Id
	// protected final static String CHINESE_CONFIG_ID = "0733eab83ebf84ad0cceb4f7bb97f363";
	// private final static String ENGLISH_CONFIG_ID = "9e69e3082b553e32273802bb772eae14";

	public enum SemantriaConfig {
		ENGLISH, CHINESE;
	}

	private final static String[] CHINESE_CONFIG_IDS = CommonConfig.getInstance().getSemantriaChiConfigId();
	private final static String[] ENGLISH_CONFIG_IDS = CommonConfig.getInstance().getSemantriaEngConfigId();

	private final RedisUtil redisUtil;
	private final Random random;
	private final boolean useConfigPool;
	private String chineseConfigId;
	private String englishConfigId;

	public SemantriaService() throws Exception {
		this(true);
	}

	/**
	 * @throws Exception
	 * 
	 */
	public SemantriaService(boolean useConfigPool) throws Exception {
		super();

		log.info("Initializing Semantria Service");
		this.useConfigPool = useConfigPool;
		redisUtil = RedisUtil.getInstance();
		random = new Random();

		if (!useConfigPool) {
			chineseConfigId = "d8c6477100b1d082282edc471b0dbf12";
			englishConfigId = "9e6f9b886e61864db9f135f19af45dab";
		}
	}

	/**
	 * Session is the root object for API calls
	 */
	public Session createSession() {
		Session session = null;
		session = Session.createSession(KEY, SECRET, new JsonSerializer());
		session.setCallbackHandler(new CallbackHandler());
		return session;
	}

	/**
	 * Subscription provides account settings
	 */
	public Subscription createSubscription(Session session) {
		Subscription subscription = null;
		subscription = session.getSubscription();
		return subscription;
	}

	public static void clearConfigMap() {
		try {
			RedisUtil.getInstance().del(StormDef.SEMANTRIA_CONFIG_HASH);
			log.info("Clear semantria config hash successfully!");
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
	}

	/**
	 * Return list of configurations
	 */
	protected List<Configuration> getConfigurations(Session session) {
		return session.getConfigurations();
	}

	protected String getChineseConfigId(int index) {
		if (chineseConfigId == null) {
			chineseConfigId = allocateConfig(SemantriaConfig.CHINESE, index);
		}
		return chineseConfigId;
	}

	protected String getEnglishConfigId(int index) {
		if (englishConfigId == null) {
			englishConfigId = allocateConfig(SemantriaConfig.ENGLISH, index);
		}
		return englishConfigId;
	}

	public String allocateConfig(SemantriaConfig config, int index) {
		if (config == null) {
			return "";
		}

		String configId = null;

		try {
			switch (config) {
				case CHINESE:
					configId = CHINESE_CONFIG_IDS[index];
					break;
				case ENGLISH:
					configId = ENGLISH_CONFIG_IDS[index];
					break;
				default:
					// Invalid config
					return null;
			}
				
			return configId;
			
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		} finally {

		}

		return null;
	}

	public Long getTransctions(Subscription subscription) {
		return subscription.getBillingSettings().getDocsBalance();
	}
}
