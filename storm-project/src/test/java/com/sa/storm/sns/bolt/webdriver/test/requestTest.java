package com.sa.storm.sns.bolt.webdriver.test;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.BasicResponseHandler;

import com.sa.common.util.HttpClientUtil;

public class requestTest {
	private static HttpClient httpClient;
	private static final String restUrl = "http://192.168.12.43:8080/facebook-rest/pub/data/persistSingleUser";

	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		String uId = "michael.castaneda.982";
		String liveIn = "Shang Hai";
		String friendNum = "1000";
		String requestResult = submitRequest(restUrl + "?uid=" + uId + "&livein=" + URLEncoder.encode(liveIn,"UTF-8") + "&friendnum=" + URLEncoder.encode(friendNum,"UTF-8"));
	}
	
	public static String submitRequest(String url) {
		httpClient = HttpClientUtil.getInstance().getClient();
		String result = new String();
		HttpRequestBase httpRequest = null;

		try {
			httpRequest = new HttpGet(url);
			final ResponseHandler<String> handler = new BasicResponseHandler();
			result = httpClient.execute(httpRequest, handler);
		} catch (Exception e) {
			if (httpRequest != null) {
				httpRequest.abort();
			}
		} finally {
			if (httpRequest != null) {
				httpRequest.releaseConnection();
			}
		}

		return result;
	}


}
