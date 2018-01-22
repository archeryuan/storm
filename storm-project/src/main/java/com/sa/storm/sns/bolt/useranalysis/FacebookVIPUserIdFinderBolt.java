package com.sa.storm.sns.bolt.useranalysis;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import com.sa.common.json.JsonUtil;
import com.sa.crawler.definition.FacebookToken;
import com.sa.crawler.message.TaskRequestMessage;
import com.sa.graph.ch.object.BackendJob;
import com.sa.graph.ch.object.JobInfo;
import com.sa.graph.ch.object.Person;
import com.sa.graph.ch.service.FBUserDataService;
import com.sa.graph.ch.service.PeopleService;
import com.sa.redis.definition.RedisDefinition;
import com.sa.redis.definition.RedisDefinition.MessageQueue;
import com.sa.redis.domain.RedisQueueMessage;
import com.sa.redis.service.RedisPubSubAckQueueService;
import com.sa.redis.util.RedisUtil;
import com.sa.storm.bolt.BaseBolt;
import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.persistence.CHVipUser;
import com.sa.storm.domain.persistence.Document;
import com.sa.storm.domain.tuple.DocumentResult;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.domain.tuple.TaskResult;
import com.sa.storm.sns.util.FindUserByEmailUtil;
import com.sa.storm.sns.util.parser.FacebookApiRequestUtil;
import com.sa.storm.util.HBaseUtil;

public class FacebookVIPUserIdFinderBolt extends BaseBolt {
	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(FacebookVIPUserIdFinderBolt.class);
	private static final String token = "CAAAAAYsX7TsBAD2b3zrrcI2cg2bHA8CtsX8KwhoMOb1RpkRLfWhH7DuYxZCWkszHhGF5XM4h3PmnNSFH9rFFtD7BXGkmZB3bIgTmhi84atv11Dlv8R4uk0B6e2pZC1762SEHCUvills4rBJFK5X5aXhRNnKZB3m3edkyTZBQMJ9OzGRdZB2wN6f2sitZBKCGGtuw5hZCbUk9Du7wSWgmMrzh49ukZCfgqvjvc00yM7Durkf7JqP6ZCZATVj";
	private static final String redisKey = "ch-mail-user";
	private PeopleService peopleService;
	private FBUserDataService fbUserDataService;
	private static SimpleDateFormat sdf = new SimpleDateFormat("MM/dd");
	private static int count = 1;
	private HBaseUtil hbaseUtil;

	private static RedisPubSubAckQueueService queueService;
	private static RedisDefinition.MessageQueue queue;

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		peopleService = new PeopleService();
		fbUserDataService = new FBUserDataService();
		queue = MessageQueue.FS_MOBILE_TOKEN_REQUEST;
		try {
			queueService = new RedisPubSubAckQueueService(queue.getWorkQueue(), queue.getProcessingQueue());
		} catch (Exception e) {
			log.error(e.getMessage(), e.toString());
		}
	}

	@Override
	public void declareOutputs(OutputFieldsDeclarer declarer) {
		declareOutputByClass(declarer, DocumentResult.class);
	}

	@Override
	public void process(Tuple input) throws Exception {

		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);

		final String docJson = request.getParamByKey(TupleDefinition.Param.DOCUMENTS);
		if (docJson != null) {
			try {
				Document doc = JsonUtil.getMapper().readValue(docJson, Document.class);
				String jobId = doc.getFileName();
				String email = doc.getEmail();
				String isLast = doc.getLastFlag();
				String hId = doc.getHistoryId();
				String userId = doc.getUserId();
				String avatar = "";
				long totalUserCount = doc.getViewCount();
				// String RedisUserId = isExistingInRedis(email);
				log.info("begin process email {}", email);
				String fbUserId = null;

//				if (isLast.equals("1")) {
//					Utils.sleep(30000);
//				}
				if (isExistingEmail(email)) {
					log.info("email in empty list {}", email);
				} else {
					Person p = peopleService.getPerson(email);
					if (null != p && null != p.getPersonId()) {
						log.info("Get email from graphDB {}, FBUserId {}", email, p.getPersonId());
						fbUserId = p.getPersonId().toString();
					} else {

						fbUserId = isExistingInRedis(email);
						if (null != fbUserId) {
							log.info("userInfo {}, {}", userId, avatar);
							// persist user information to GraphDB
							// saveUserInfoToGraphDB(FBUserId, userId, email, hId);
						} else {
							log.info("using crawler, count {} ", count);
							count++;
							// String fbAccoount = "";
							// while (null == (fbAccoount = getAvalibleAccount())) {
							// Utils.sleep(20000);
							// }
							// log.info("get the account " + fbAccoount);
							// String accountInfo[] = fbAccoount.split(":");
							// String username = null;
							// try {
							// username = PythonCrawlerUtil.getInstance().getUserIdbyPythonCrawler("/home/sa/fb/fb-usa.py",
							// accountInfo[0], accountInfo[1], "/home/sa/fb/" + accountInfo[0] + ".txt",
							// "https://www.facebook.com/search/str/" + email + "/keywords_users");
							// } catch (Exception e) {
							// log.error(e.getMessage(), e.toString());
							// username = null;
							// }
							// if (!username.trim().equals("empty") && null != username) {
							// log.info("username  " + username);
							// String json = submitSingleUserRequest(username);
							// JSONObject userInfo = new JSONObject(json);
							// FBUserId = userInfo.optString("id");
							// }
							
							String fbMobileToken = "";
							while (null == (fbMobileToken = getAvalibleAccount())) {
								Utils.sleep(30000);
							}
							log.info("get the token {}", fbMobileToken);
							FindUserByEmailUtil fbUtil = new FindUserByEmailUtil();
							fbUserId = fbUtil.mobileTokenToFindUser(email, fbMobileToken);
//							Utils.sleep(30000);
						}

					}

				}

				if (null != fbUserId) {
					log.info("save email {} to graphdb , FBUId {}", email, fbUserId);
					saveUserInfoToGraphDB(fbUserId, userId, email, hId);
				} else {
					saveUserEmail(email);
				}
				DocumentResult documentResult = new DocumentResult(request);
				Document nextTaskDoc = new Document();
				nextTaskDoc.setFileName(jobId);
				nextTaskDoc.setEmail(email);
				nextTaskDoc.setLastFlag(isLast);
				nextTaskDoc.setUserId(userId);
				nextTaskDoc.setUserName(fbUserId);
				nextTaskDoc.setHistoryId(hId);
				documentResult.setDocument(nextTaskDoc);
				nextTaskDoc.setViewCount(totalUserCount);
				emit(input, documentResult, "emit to tokenization");
			} catch (Exception ex) {
				log.error("error {}", ex.toString());
				ex.printStackTrace();
				TaskResult taskResult = new TaskResult(request, "exception when crawling Facebook UserId",
						TupleDefinition.Result.FAIL.getCode());
				emit(input, taskResult);
			}

		} else {
			TaskResult taskResult = new TaskResult(request, "process successfully", TupleDefinition.Result.SUCCESS.getCode());
			emit(input, taskResult);

		}

	}

	private boolean isExistingEmail(String email) {
		hbaseUtil = HBaseUtil.getInstance();
		HTable table;
		try {
			table = (HTable) hbaseUtil.getTable(CHVipUser.HBASE_TABLE);

			Get get = new Get(Bytes.toBytes(email.trim()));
			Result hbaseResult = table.get(get);
			if (hbaseResult != null && !hbaseResult.isEmpty()) {
				return true;
			}
		} catch (IOException e) {
			log.error("error {}", e.toString());
			e.printStackTrace();
		}
		return false;
	}

	private void saveUserEmail(String email) {
		CHVipUser user = new CHVipUser(email);
		Put put = user.toHBasePut();
		try {
			HBaseUtil.getInstance().executePut(CHVipUser.HBASE_TABLE, put);
		} catch (IOException e) {
			log.error("error {}", e.toString());
			e.printStackTrace();
		}
		log.info("persist empty email {} into hbase", email);
	}

	private String isExistingInRedis(String email) {
		try {
			RedisUtil redisUtil = RedisUtil.getInstance();
			if (null != redisUtil.hget(redisKey, email)) {
				return redisUtil.hget(redisKey, email);
			}
		} catch (Exception e) {
			log.error("error {}", e.toString());
			e.printStackTrace();
		}
		return null;
	}

	private String getAvalibleAccount() throws Exception {
		RedisQueueMessage message = null;
		TaskRequestMessage requestMsg = null;
		message = queueService.dequeue();
		if (message == null) {
			log.info("no account");
			return null;
		}

		String msg = message.getContent();
		queueService.ack(msg);
		requestMsg = JsonUtil.getMapper().readValue(msg, TaskRequestMessage.class);
		final String docJson = requestMsg.getParams().get("docs");
		Document doc = JsonUtil.getMapper().readValue(docJson, Document.class);
		String account = doc.getContent();
		return account;
	}

	private void saveToRedis(String email, String value) {
		RedisUtil redisUtil;
		try {
			redisUtil = RedisUtil.getInstance();
			redisUtil.hset(redisKey, email, value);
		} catch (Exception e) {
			log.error("error {}", e.toString());
			e.printStackTrace();
		}
	}

	private void saveUserInfoToGraphDB(String FBUId, String userId, String email, String hId) {
		try {

			String json = submitSingleUserRequest(FBUId);
			log.info("FBUid {} ", FBUId);

			Person person = parserUser(json, email);
			BackendJob job = new BackendJob();
			job.setJobId(Long.parseLong(hId));
			fbUserDataService.saveFBUser(person, job);
		} catch (Exception e) {
			log.error("error {}", e.toString());
			e.printStackTrace();
		}
	}

	private void ExistingUserEdit(Long userId, Long hId) {
		try {
			log.info("sourceId {}, hId {}", userId, hId);
			BackendJob job = new BackendJob();
			job.setJobId(hId);
		} catch (Exception e) {
			log.error("error {}", e.toString());
			e.printStackTrace();
		}
	}

	private String submitSingleUserRequest(String userId) {
		String url = "https://graph.facebook.com/" + userId + "?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
		String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, token);
		return json;
	}

	private Person parserUser(String json, String email) throws JSONException {

		Person p = new Person();
		try {
			JSONObject userInfo = new JSONObject(json);
			String id = userInfo.optString("id");
			p.setPersonId(Long.parseLong(id));
			p.setPEmail1(email);
			String name = userInfo.optString("name");
			p.setName(name);
			String username = userInfo.optString("username");
			p.setFbStrId(username);

			String gender = null;
			if (null != userInfo.optString("gender")) {
				gender = userInfo.optString("gender");
				p.setGender(gender);
			}
			String hometown = null;
			if (null != userInfo.optJSONObject("hometown")) {
				hometown = userInfo.optJSONObject("hometown").optString("name");
				p.setHomeTown(hometown);
			}

			String location = null;
			if (null != userInfo.optJSONObject("location")) {
				location = userInfo.optJSONObject("location").optString("name");
				p.setCity(location);
			}

			String schoolName = null;
			if (null != userInfo.optJSONArray("education")) {
				JSONArray educations = userInfo.optJSONArray("education");
				JSONObject school = userInfo.optJSONArray("education").getJSONObject(educations.length() - 1);
				schoolName = school.optJSONObject("school").optString("name");
				p.setCollege(schoolName);
			}
			String birthday = userInfo.optString("birthday");
			if (null != birthday && "" != birthday) {
				try {
					Date BDate = sdf.parse(birthday);
					p.setBDay(BDate);
					log.info("birthdayDate {} ", BDate.toString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			String interestedIn = userInfo.optString("interested_in");

			if (null != interestedIn && "" != interestedIn) {
				log.info("genderInterest {} ", interestedIn);
				p.setGenderInt(extractEnglish(interestedIn));
			}
			JSONArray languages = userInfo.optJSONArray("languages");
			if (null != languages) {
				ArrayList<String> languageList = new ArrayList<String>();
				for (int i = 0; i < languages.length(); i++) {
					languageList.add(languages.getJSONObject(i).optString("name"));
				}
				p.setLang(languageList);
			}

			String religion = userInfo.optString("religion");
			if (null != religion && "" != religion) {
				p.setReligion(extractEnglish(religion));
			}

			String political = userInfo.optString("political");
			if (null != political && "" != political) {
				p.setPolitics(extractEnglish(political));
			}

			String relation = userInfo.optString("relationship_status");
			if (null != relation || "" != relation) {
				p.setRelation(relation);
			}
			String phone = userInfo.optString("mobile_phone");
			if (null != phone && "" != phone) {
				p.setPPhone(Long.parseLong(phone));
			}
			String website = userInfo.optString("website");
			if (null != phone && "" != phone) {
				p.setWebSite(website);
			}

			JSONObject address = userInfo.optJSONObject("address");
			if (null != address) {
				p.setAddr(address.optString("street"));
			}

			JSONArray works = userInfo.optJSONArray("work");
			if (null != works) {
				JSONObject work = works.optJSONObject(0);
				JobInfo jobInfo = new JobInfo();
				if (null != work.optJSONObject("employer")) {
					jobInfo.setCompanyName(work.optJSONObject("employer").optString("name"));
				}
				if (null != work.optJSONObject("location")) {
					jobInfo.setCity(work.optJSONObject("location").optString("name"));
				}
				if (null != work.optJSONObject("position")) {
					jobInfo.setPosition(work.optJSONObject("position").optString("name"));
				}
				try {
					String JobInfoJson = JsonUtil.getMapper().writeValueAsString(jobInfo);
					p.setJobInfoJSON(JobInfoJson);
					log.info(JobInfoJson);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception ex) {
			log.error("error {}", ex.toString());
			ex.printStackTrace();
		}
		// ArrayList<Person> friends = parseFriends(id);
		// if (null != friends) {
		// p.setFriends(friends);
		// }
		return p;

	}

	private static String extractEnglish(String str) {
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < str.length(); i++) {
			char a = str.charAt(i);
			int c = (int) a;
			if ((c >= 65 && c <= 90) || (c >= 97 && c <= 122) || c == 32) {
				result.append(a);
			}
		}

		return result.toString();
	}

	private ArrayList<Person> parseFriends(String userId) throws JSONException {
		ArrayList<Person> friendsList = new ArrayList<Person>();
		try {
			String url = "https://graph.facebook.com/" + userId + "/friends?access_token=" + FacebookToken.MobileApiToken.USA_MOBILE_TOKEN;
			String json = FacebookApiRequestUtil.getInstance().submitMobileSimulatorRequest(url, token);
			JSONObject friendList = new JSONObject(json);
			JSONArray friendArray = friendList.optJSONArray("data");
			String friendStr = " ,";
			for (int i = 0; i < friendArray.length(); i++) {
				String name = friendArray.getJSONObject(i).optString("name");
				String id = friendArray.getJSONObject(i).optString("id");
				Person friend = new Person();
				friend.setPersonId(Long.parseLong(id));
				friend.setName(name);
				friendsList.add(friend);
			}
		} catch (Exception ex) {
			return null;
		}
		return friendsList;
	}

	@Override
	public void onError(Tuple input, Exception e) throws Exception {
		// TODO Auto-generated method stub
		TaskRequest request = (TaskRequest) getInputByClass(input, TaskRequest.class);
		TaskResult error = new TaskResult(request, "Error in fliter doc", e, TupleDefinition.Result.FAIL.getCode());
		emit(input, error);
	}

}
