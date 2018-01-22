package com.sa.competitorhunter.token;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.DomElement;
import com.gargoylesoftware.htmlunit.html.DomNodeList;
import com.gargoylesoftware.htmlunit.html.HtmlButton;
import com.gargoylesoftware.htmlunit.html.HtmlCheckBoxInput;
import com.gargoylesoftware.htmlunit.html.HtmlElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.html.HtmlPasswordInput;
import com.gargoylesoftware.htmlunit.html.HtmlSubmitInput;
import com.gargoylesoftware.htmlunit.html.HtmlTextInput;
import com.gargoylesoftware.htmlunit.util.Cookie;

public class FacebookAccessToken {

	private static final Logger log = LoggerFactory.getLogger(FacebookAccessToken.class);

	private static final int MAX_RETRY_COUNT = 2;
	private static final int RETRY_INTERVAL = 5000;

	// private static FacebookAccessTokenRenewer instance;

	private final String username;
	private final String password;

	private Set<Cookie> cookies;

	private WebClient webClient = null;

	/**
	 * @param username
	 * @param password
	 */
	public FacebookAccessToken(String username, String password) {
		this.username = username;
		this.password = password;
		this.webClient = buildWebClient();
	}

	// public static FacebookAccessTokenRenewer getInstance() {
	// if (instance == null) {
	// synchronized (FacebookAccessTokenRenewer.class) {
	// if (instance == null) {
	// instance = new FacebookAccessTokenRenewer();
	// }
	// }
	// }
	// return instance;
	// }

	public String renewToken() throws Exception {
		int retryCount = 1;
	//	WebClient webClient = null;

		try {
			if (webClient == null){
				webClient = buildWebClient();
			}

			do {
				log.info("Perform {} renewal", retryCount);

				if (CollectionUtils.isEmpty(readCookies())) {
					log.info("Cannot obtain valid cookies for facebook");
				} else {
					applyCookies(webClient);

					HtmlPage apiPage = webClient.getPage(FACEBOOK_DEV_URL);

					DomNodeList<HtmlElement> scripts = apiPage.getDocumentElement().getElementsByTagName(FACEBOOK_DEV_SCRIPT);
					String token = null;

					for (HtmlElement htmlElement : scripts) {
						String sc = htmlElement.asXml();

						if (sc.indexOf(FACEBOOK_GE) != -1) {
							String startStr = FACEBOOK_GE;
							int start = sc.indexOf(startStr);
							String ge = sc.substring(start);
							for (int i = 0; i <= 6; i++) {
								ge = ge.substring(ge.indexOf(",") + 1);
							}
							ge = ge.substring(1);
							ge = ge.substring(ge.indexOf(",\"") + 2, ge.indexOf("\","));
							token = ge;
							break;
						}
					}
					log.info("token:" + token);
					if (token != null && !token.trim().isEmpty()) {
						log.info("Updated token obtained: {}", token);
						return token;
					} else {
						log.info("No token can be obtained");
					}
				}

				if (retryCount < MAX_RETRY_COUNT) {
					log.info("Retry again after {}ms", RETRY_INTERVAL);
					retryCount++;
					saveCookies(null);
					Thread.sleep(RETRY_INTERVAL);
				} else {
					log.info("Retried for {} times already, do not retry anymore", retryCount);
					break;
				}
			} while (true);

			return null;
		} finally {
			cleanUpWebClient(webClient);
		}
	}

	/**
	 * Log in Fcaebook
	 * @return
	 * @throws Exception
	 */
	public boolean login() throws Exception {

		try {
			if (webClient == null){
				webClient = buildWebClient();
			}

			log.info("Login facebook now...");
			HtmlPage page = webClient.getPage(FACEBOOK_LOGIN_URL);

			if (page != null) {
				log.info("Facebook login page html loaded");
				HtmlTextInput userNameInput = (HtmlTextInput) page.getElementById(FACEBOOK_LOGIN_EMAIL);
				HtmlPasswordInput passwordInput = (HtmlPasswordInput) page.getElementById(FACEBOOK_LOGIN_PWD);
				HtmlCheckBoxInput persistBox = (HtmlCheckBoxInput) page.getElementById(FACEBOOK_LOGIN_KEEP_ME_LOGIN);
				persistBox.setChecked(true);

				userNameInput.setText(username);
				passwordInput.setText(password);
				DomElement submitLabel = page.getElementById(FACEBOOK_LOGIN_BTN);

				HtmlSubmitInput submitInput = (HtmlSubmitInput) submitLabel.getElementsByTagName(FACEBOOK_LOGIN_INPUT).get(0);
				submitInput.click();

				saveCookies(webClient.getCookieManager().getCookies());
				log.info("Facebook login successfully");
				return true;
			} else {
				log.info("Facebook login page cannot be loaded");
				return false;
			}
		} finally {
			cleanUpWebClient(webClient);
		}
	}

	/**
	 * Read cookies from global variable
	 * 
	 * @return
	 * @throws Exception
	 */
	private synchronized Set<Cookie> readCookies() throws Exception {
		if (CollectionUtils.isEmpty(cookies)) {
			login();
		}
		return cookies;
	}

	/**
	 * Save cookies to global variable
	 * 
	 * @param newCookies
	 * @throws Exception
	 */
	private synchronized void saveCookies(Set<Cookie> newCookies) throws Exception {
		cookies = newCookies;
	}

	/**
	 * Apply cookies to a HTMLUnit webclient
	 * 
	 * @param webClient
	 * @throws Exception
	 */
	private synchronized void applyCookies(WebClient webClient) throws Exception {
		for (Cookie cookie : readCookies()) {
			webClient.getCookieManager().addCookie(cookie);
		}
	}

	private WebClient buildWebClient() {
		WebClient webClient = new WebClient();
		webClient.addRequestHeader(FACEBOOK_USER_AGENT, FACEBOOK_USER_AGENT_VALUE);
		webClient.getOptions().setCssEnabled(false);
		webClient.getOptions().setJavaScriptEnabled(true);
		webClient.getOptions().setThrowExceptionOnScriptError(false);
		webClient.getOptions().setThrowExceptionOnFailingStatusCode(true);
		webClient.getOptions().setRedirectEnabled(true);
		webClient.getOptions().setTimeout(1200 * 1000);
		webClient.setJavaScriptTimeout(1200 * 1000);
		webClient.waitForBackgroundJavaScript(1200 * 1000);
		webClient.setAjaxController(new NicelyResynchronizingAjaxController());

		return webClient;
	}

	private void cleanUpWebClient(WebClient webClient) {
		if (webClient != null) {
			webClient.closeAllWindows();
		}
	}

	/*
	 * Get Social Baker Access by Facebook Account
	 */
	public boolean getSBAuthorization() throws Exception {

		try {
			if (webClient == null){
				webClient = buildWebClient();
			}

			if (CollectionUtils.isEmpty(readCookies())) {
				log.info("Cannot obtain valid cookies for facebook");
			} else {
				applyCookies(webClient);
			}
			log.info("Login Social Baker now...");
			HtmlPage page = webClient.getPage(SOCIAL_BAKER_AUTU_URL);
			if (page != null) {
				HtmlButton submitInput = (HtmlButton) page.getElementsByTagName(FACEBOOK_OK_BUTTON).get(1);
				if (submitInput != null){
					log.info("submitInput is not null");
					submitInput.click();
				}
				return true;
			}
			
		} catch (Exception ex){
		}
		finally {
			cleanUpWebClient(webClient);
		}
		return false;

	}

	public HtmlPage getHTMLPage(String url) throws Exception {

		try {
			if (webClient == null){
				webClient = buildWebClient();
			}

			if (CollectionUtils.isEmpty(readCookies())) {
				log.info("Cannot obtain valid cookies for facebook");
			} else {
				applyCookies(webClient);
			}
			HtmlPage page = webClient.getPage(url);
			if (page != null) {
				System.out.println(page.asText());
				return page;
			} else {
				log.info("Cannot get the page");
				return null;
			}
		} finally {
			cleanUpWebClient(webClient);
		}
	}

	private static final String SOCIAL_BAKER_AUTU_URL = "https://www.facebook.com/dialog/oauth?display=popup&response_type=code&redirect_uri=http%3A%2F%2Faccount.socialbakers.com%2Flogin%2Ffacebook%3Fdisplay%3Dpopup%26product%3D12%26cb%3Dhttp%253A%252F%252Fwww.socialbakers.com%252Fsign%252Fauthenticate%253Fmethod%253Dpopup%2526ref%253Dwww-fe-modal%2526backlink%253Dhttp%25253A%25252F%25252Fwww.socialbakers.com%25252F&scope=user_birthday%2Cuser_hometown%2Cuser_location%2Cuser_work_history%2Cemail&client_id=111353382239227";
	private static final String FACEBOOK_DEV_URL = "http://developers.facebook.com/tools/explorer";
	private static final String FACEBOOK_DEV_SCRIPT = "script";
	private static final String FACEBOOK_GE = "\"GraphExplorer\"";
	// private static final String FACEBOOK_COOKIES_FILE =
	// "facebook_cookie.txt";
	private static final String FACEBOOK_USER_AGENT = "User-Agent";
	private static final String FACEBOOK_USER_AGENT_VALUE = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0";
	private static final String FACEBOOK_LOGIN_URL = "http://www.facebook.com/login.php";
	private static final String FACEBOOK_LOGIN_EMAIL = "email";
	private static final String FACEBOOK_LOGIN_PWD = "pass";
	private static final String FACEBOOK_LOGIN_KEEP_ME_LOGIN = "persist_box";
	private static final String FACEBOOK_LOGIN_BTN = "loginbutton";
	private static final String FACEBOOK_LOGIN_INPUT = "input";
	private static final String FACEBOOK_OK_BUTTON = "button";

	public static void main(String[] args) throws Exception {
		FacebookAccessToken access = new FacebookAccessToken("639439551863", "9439551863");
		access.login();
		access.getSBAuthorization();
		access.getHTMLPage("http://www.socialbakers.com//statistics/facebook/pages/detail/123934470973808-ovolo-hotels");

	}
}
