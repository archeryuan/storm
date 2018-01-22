package com.sa.storm.sns.bolt.webdriver.test;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.BasicResponseHandler;

import com.sa.common.util.HttpClientUtil;

public class restfulTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String result = submitRequest("http://127.0.0.1:8080/sa-external-api/extAPI/findUserByPageId?pageid=08_nike");
		System.out.println(result);
	}
	
	public static String submitRequest(String url) {
		HttpClient httpClient = HttpClientUtil.getInstance().getClient();
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
