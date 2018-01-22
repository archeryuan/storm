package com.sa.storm;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.sa.storm.domain.persistence.TrackLog;
import com.sa.storm.service.TrackLogService;

public class TrackLogExporter {

	/**
	 * Export track log information
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		final int numRecord = Integer.parseInt(args[0]);
		final String exportFilePath = args[1];

		TrackLogService trackLogService = new TrackLogService();
		List<TrackLog> trackLogs = trackLogService.getLogsByDate(new Date(), numRecord);

		File file = new File(exportFilePath);
		FileOutputStream output = null;

		try {
			output = new FileOutputStream(file);
			int count = 0;

			IOUtils.write(
					"\"Request ID\",\"Parent Request ID\",\"Source Type\",\"Result Code\",\"Type\",\"Params\",\"Contexts\",\"Histories\"\n",
					output, "UTF-8");

			for (TrackLog trackLog : trackLogs) {
				final String csvLine = new StringBuffer(quote(trackLog.getRequestId())).append(",")
						.append(quote(trackLog.getParentRequestId())).append(",").append(quote(trackLog.getSourceType())).append(",")
						.append(quote(String.valueOf(trackLog.getResultCode()))).append(",")
						.append(quote(String.valueOf(trackLog.getType()))).append(",").append(quote(trackLog.getParams())).append(",")
						.append(quote(trackLog.getContexts())).append(",").append(quote(trackLog.getHistories().replaceAll("\n", " ")))
						.toString();
				IOUtils.write(csvLine + "\n", output, "UTF-8");
				count++;

				System.out.println(trackLog);
			}

			System.out.println("Total log processed: " + count);
		} catch (Exception e) {
			IOUtils.closeQuietly(output);
		}
	}

	public static String quote(String input) {
		return new StringBuffer("\"").append(input).append("\"").toString();
	}
}
