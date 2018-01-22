package com.sa.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.sa.common.json.JsonUtil;
import com.sa.crawler.message.TaskRequestMessage;
import com.sa.redis.definition.RedisDefinition.Priority;
import com.sa.redis.service.RedisPubSubAckQueueService;

/**
 * Manually trigger a request to restlet
 * 
 * @param args
 *            args[0] - Work queue (e.g. str-keyword-wq)
 *            args[1] - Processing queue (e.g. str-keyword-pq)
 *            args[2] - sourceType (e.g. 03 04 05 etc.)
 *            args[3] - type (e.g. 1 for keyword, 2 for vipuser etc., refer to TaskType enum)
 *            args[4] - priority (e.g. 0 for normal, 1 for high)
 *            args[5] - number of parameter values (e.g. 1) 
 *            args[6] - number of state values (e.g. 1) 
 *            args[7+i] - parameter keys (e.g. keyword, uId) 
 *            args[7+i*2] - parameter values (e.g. 'china', '06_likaifu') 
 *            args[...] - state keys (e.g. lrt)
 *            args[...] - state values (e.g. 123465)
 *            
 *            e.g. ManualRequestSubmitter str-keyword-wq str-keyword-pq 05 1 0 1 1 keyword Crimea lrt 11002541
 * 
 * @throws Exception
 */
public class ManualRequestSubmitter {

	public static void main(String[] args) throws Exception {
		try {
			final String workQueue = args[0];
			final String processingQueue = args[1];

			final String sourceType = args[2];
			final String type = args[3];
			final String priority = args[4];
			final int paramNum = Integer.parseInt(args[5]);
			final int stateNum = Integer.parseInt(args[6]);

			List<String> paramKeys = new ArrayList<String>();
			List<String> paramValues = new ArrayList<String>();

			List<String> stateKeys = new ArrayList<String>();
			List<String> stateValues = new ArrayList<String>();

			int i = 7;
			while (i < 6 + paramNum * 2) {
				paramKeys.add(args[i]);
				paramValues.add(args[i + 1]);
				i += 2;
			}

			int j = i;
			while (j < i + stateNum * 2) {
				stateKeys.add(args[j]);
				stateValues.add(args[j + 1]);
				j += 2;
			}

			final String id = UUID.randomUUID().toString();

			TaskRequestMessage request = new TaskRequestMessage();
			request.setId(id);
			request.setSourceType(sourceType);
			request.setType(Integer.parseInt(type));
			request.setPriority(Integer.parseInt(priority));

			Map<String, String> params = new HashMap<String, String>();
			for (int k = 0; k < paramKeys.size(); k++) {
				params.put(paramKeys.get(k), paramValues.get(k));
			}
			request.setParams(params);

			Map<String, String> states = new HashMap<String, String>();
			for (int k = 0; k < stateKeys.size(); k++) {
				states.put(stateKeys.get(k), stateValues.get(k));
			}
			request.setStates(states);

			String requestInJson = JsonUtil.getMapper().writeValueAsString(request);

			RedisPubSubAckQueueService service = new RedisPubSubAckQueueService(workQueue, processingQueue);
			service.enqueue(requestInJson, Priority.getPriority(Integer.parseInt(priority)));

			System.out.println("Message " + id + " sent successfully");
		} catch (Exception e) {
			System.out.println("Error in manually submit request");
			e.printStackTrace();
		}
	}
}
