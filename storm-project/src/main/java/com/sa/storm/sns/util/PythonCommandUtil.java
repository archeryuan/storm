package com.sa.storm.sns.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.StringUtils;

import com.sa.storm.definition.TupleDefinition;
import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.sns.domain.persistence.LoginUser;
import com.sa.storm.sns.domain.phantom.CommandResult;

public class PythonCommandUtil extends AbstractCommandUtil {

	private static final String COOKIE_FILE_SUFFIX = ".pkl";

	private static final String SEARCH_FANS_PY = "fb-fans.py";

	private static PythonCommandUtil instance;

	private PythonCommandUtil() {

	}

	public static PythonCommandUtil getInstance() {
		if (instance == null) {
			synchronized (PythonCommandUtil.class) {
				if (instance == null)
					instance = new PythonCommandUtil();
			}
		}
		return instance;
	}

	public CommandResult executeSearchFansCommand(TaskRequest request, LoginUser loginUser, String convertFromCharset,
													String convertToCharset, String workingDir, String filePath) throws Exception {
		Map<String, String> params = new HashMap<String, String>();
		final String url = ConvertEncodingUtil.convert(convertFromCharset, convertToCharset,
				request.getParamByKey(TupleDefinition.Param.URL));
		params.put("url", quote(url));
		params.put("file", quote(filePath));
		
		return executeCommand(SEARCH_FANS_PY, loginUser, request, params, workingDir);
	}

	protected String getResourcesPath() throws ConfigurationException {
		return ChCrawlerConfiguration.getInstance().getPythonResourcesPattern();
	}

	@Override
	protected String getTemplatesDir(String workingDir) throws Exception {
		return ChCrawlerConfiguration.getInstance().getPythonTemplatesPath(workingDir);
	}

	@Override
	protected String getCookiesDir(String workingDir) throws Exception {
		return ChCrawlerConfiguration.getInstance().getPythonCookiesPath(workingDir);
	}

	@Override
	protected String parse(String commandOutput) throws Exception {
		return commandOutput;
	}

	@Override
	protected String getCookiesPath(String sourceType, String loginUserId, String taskRequestId, String cookiesDir) throws Exception {
		StringBuilder path = new StringBuilder(cookiesDir).append(sourceType);

		if (!StringUtils.isEmpty(loginUserId)) {
			path.append("_").append(loginUserId);
		}

		if (!StringUtils.isEmpty(taskRequestId)) {
			path.append("_").append(taskRequestId);
		}

		path.append(COOKIE_FILE_SUFFIX);

		return path.toString();
	}

	@Override
	protected String buildCommandLine(String scriptFilePath, String cookiesFilepath, Map<String, String> commandParams) throws Exception {
		if (StringUtils.isEmpty(scriptFilePath))
			throw new Exception("scriptFilePath is empty");
		
		StringBuilder command = new StringBuilder(ChCrawlerConfiguration.getInstance().getPythonRuntime());
		command.append(" ").append(scriptFilePath);
		
		command.append("-cookie").append(" ").append(cookiesFilepath);

		if (null != commandParams) {
			for (Entry<String, String> param : commandParams.entrySet()) {
				command.append("-").append(param.getKey()).append(" ").append(param.getValue());
			}
		}

		return command.toString();
	}
	
	public String getResultTemporaryFilePath(String workingDir) throws Exception {
		return ChCrawlerConfiguration.getInstance().getResultTemporaryFilePath(workingDir);
	}

}
