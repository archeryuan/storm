package com.sa.storm.sns.util;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import com.sa.storm.domain.tuple.TaskRequest;
import com.sa.storm.sns.definition.CommandStatusDefinition;
import com.sa.storm.sns.definition.PhantomParserDefinition;
import com.sa.storm.sns.domain.persistence.LoginUser;
import com.sa.storm.sns.domain.phantom.CommandResult;

public abstract class AbstractCommandUtil {

	private static final Logger log = LoggerFactory.getLogger(AbstractCommandUtil.class);

	protected abstract String getResourcesPath() throws Exception;

	protected abstract String getTemplatesDir(String workingDir) throws Exception;

	protected abstract String getCookiesDir(String workingDir) throws Exception;
	
	protected abstract String getCookiesPath(String sourceType, String loginUserId, String taskRequestId, String cookiesDir) throws Exception;

	protected abstract String parse(String commandOutput) throws Exception;

	protected abstract String buildCommandLine(String scriptFilePath, String cookiesFilepath, Map<String, String> commandParams)
			throws Exception;

	/**
	 * Extract phantom templates file from the jar into physical directory
	 * 
	 * @param workingDir
	 * @throws Exception
	 */
	public void deployTemplates(String workingDir) throws Exception {
		PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
		Resource[] resources = resolver.getResources(getResourcesPath());

		// Clean up the templates directory
		final String templatesDir = getTemplatesDir(workingDir);
		log.info("templatesDir: {}", templatesDir);
		(new File(templatesDir)).mkdirs();
		FileUtils.cleanDirectory(new File(templatesDir));

		// Copy files from jar
		for (Resource resource : resources) {
			final String fileName = resource.getFilename();
			InputStream is = resource.getInputStream();
			OutputStream os = new FileOutputStream(new File(templatesDir + fileName));

			IOUtils.copy(is, os);

			log.info("Finished deploy template file {}", fileName);
		}
	}

	/**
	 * Clean the phantom templates stored in the working dir
	 * 
	 * @param workingDir
	 * @throws Exception
	 */
	public void cleanupTemplates(String workingDir) throws Exception {
		FileUtils.deleteDirectory(new File(workingDir));
	}

	/**
	 * Execute a command and pass back the result information
	 * 
	 * @return
	 * @throws Exception
	 */
	public CommandResult executeCommand(String scriptFilePath, LoginUser loginUser, TaskRequest request,
											Map<String, String> commandParams, String workingDir) throws Exception {
		DefaultExecutor exec = getExecutor(getTemplatesDir(workingDir));
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		File cookiesFile = null;
		String command = null;

		try {
			String cookiesFilePath = null;

			if (loginUser != null) {
				cookiesFilePath = getCookiesPath(loginUser.getSourceType(), loginUser.getUserId(), request.getRequestId(),
						getCookiesDir(workingDir));
				cookiesFile = prepareCookiesFile(cookiesFilePath, loginUser.getCookies());
				log.info("Prepare cookies file {}, cookies: {}", cookiesFilePath, loginUser.getCookies());
			} else {
				cookiesFilePath = getCookiesPath(request.getSourceType(), getCookiesDir(workingDir));
				cookiesFile = prepareCookiesFile(cookiesFilePath, null);
				log.info("Prepare cookies file {}", cookiesFilePath);
			}

			command = buildCommandLine(scriptFilePath, cookiesFilePath, commandParams);

			log.info("Execute command: {}", command);
			
			if (StringUtils.isEmpty(command))
				throw new Exception("Build command error");

			PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream);
			exec.setStreamHandler(streamHandler);

			CommandLine commandLine = CommandLine.parse(command);
			final int returnCode = exec.execute(commandLine);
			final String commandOutput = outputStream.toString("UTF-8");

			log.info("Command output: {}, for command: {}", commandOutput, command);

			CommandResult commandResult = new CommandResult();

			// Login cookies and last access time
			if (loginUser != null) {
				final String updatedCookies = FileUtils.readFileToString(cookiesFile);
				loginUser.setCookies(updatedCookies);
				loginUser.setLastAccessTime(System.currentTimeMillis());
				commandResult.setLoginUser(loginUser);
			}

			// Return code
			commandResult.setReturnCode(returnCode);
			log.info("Return code: {}", returnCode);

			if (returnCode != CommandStatusDefinition.Result.OK.getCode()) {
				return commandResult;
			}

			// Return message
			String returnMsg = parse(commandOutput);
			commandResult.setReturnMsg(returnMsg);

			log.info("Return message: {}", returnMsg);

			return commandResult;
		} catch (Exception e) {
			if (exec != null && exec.getWatchdog() != null && exec.getWatchdog().killedProcess()) {
				log.error("Executed command killed due to timeout: " + command, e);
			} else {
				log.error("Executed command failed: " + command, e);
			}
			throw e;
		} finally {
			exec.setStreamHandler(null);
			outputStream.close();

			if (loginUser != null) {
				deleteCookiesFile(cookiesFile);
			}
		}
	}

	protected String quote(String input) {
		return new StringBuilder("\"").append(input).append("\"").toString();
	}

	private DefaultExecutor getExecutor(String templatesBasePath) throws Exception {
		DefaultExecutor exec = new DefaultExecutor();

		// Enforce timeout for the process to prevent hangs
		ExecuteWatchdog watchDog = new ExecuteWatchdog(ChCrawlerConfiguration.getInstance().getPythonTimeout());

		exec.setWatchdog(watchDog);
		exec.setWorkingDirectory(new File(templatesBasePath));
		exec.setExitValues(PhantomParserDefinition.Result.getAllCodes());

		return exec;
	}

	private File prepareCookiesFile(String cookiesFilePath, String cookies) throws Exception {
		File cookiesFile = new File(cookiesFilePath);
		cookiesFile.getParentFile().mkdirs();
		cookiesFile.createNewFile();

		if (!StringUtils.isEmpty(cookies)) {
			BufferedWriter writer = null;
			try {
				writer = new BufferedWriter(new FileWriter(cookiesFile));
				writer.write(cookies);
			} finally {
				if (writer != null) {
					writer.close();
				}
			}
		}

		return cookiesFile;
	}

	private void deleteCookiesFile(File cookiesFile) throws Exception {
		if (cookiesFile == null) {
			return;
		}

		if (cookiesFile.delete()) {
			log.info("Deleted the cookie file: {}", cookiesFile.getPath());
		} else {
			throw new IllegalArgumentException("Failed to delete file: " + cookiesFile.getPath());
		}
	}

	private String getCookiesPath(String sourceType, String cookiesDir) throws Exception {
		return getCookiesPath(sourceType, null, null, cookiesDir);
	}

	
}
