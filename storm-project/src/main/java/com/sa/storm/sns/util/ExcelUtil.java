/**
 *Copyright(C) ©2012 Social Hunter. All rights reserved.
 *
 */
package com.sa.storm.sns.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.hssf.record.crypto.Biff8EncryptionKey;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description Save data into excel.
 * @author Luke
 * @created date 2015-4-24
 * @modification history<BR>
 *               No. Date Modified By <B>Why & What</B> is modified
 * 
 * @see
 */
public class ExcelUtil {

	private static final Logger log = LoggerFactory.getLogger(ExcelUtil.class);

	private ExcelUtil() {

	}

	private static class Holder {
		private static ExcelUtil instance = new ExcelUtil();
	}

	public static ExcelUtil getInstance() {
		return Holder.instance;
	}

	public static class ExcelRow {
		private Map<Integer, String> cell2Value;

		public ExcelRow() {
			cell2Value = new HashMap<Integer, String>();
		}

		public void add(int cell, String value) {
			cell2Value.put(cell, value);
		}

		public Map<Integer, String> getCell2Value() {
			if (null == cell2Value)
				cell2Value = new HashMap<Integer, String>();

			return cell2Value;
		}

		public void setCell2Value(Map<Integer, String> cell2Value) {
			this.cell2Value = cell2Value;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append("[");

			if (null != cell2Value) {
				Integer cell;
				String value;
				int count = 0;
				for (Entry<Integer, String> entry : cell2Value.entrySet()) {
					cell = entry.getKey();
					value = entry.getValue();
					if (null != cell) {
						if (count > 0)
							sb.append(",");

						sb.append(cell).append(":").append(value);
						++count;

					}

				}
			}

			sb.append("]");

			return sb.toString();
		}

	}

	public List<ExcelRow> load(String fileFullPath, String sheetName, int maxRow) {
		if (StringUtils.isEmpty(fileFullPath)) {
			log.error("File path is empty");
			return null;
		}

		boolean isVersionAfter2007 = false;
		if (fileFullPath.endsWith(".xls"))
			isVersionAfter2007 = false;
		else if (fileFullPath.endsWith(".xlsx"))
			isVersionAfter2007 = true;
		else {
			log.error("File is not excel");
			return null;
		}

		File file = new File(fileFullPath);
		Workbook workbook = null;
		try {
			workbook = create(new FileInputStream(file), isVersionAfter2007, false, null);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			return null;
		}

		Sheet sheet;
		if (StringUtils.isEmpty(sheetName))
			sheet = workbook.getSheetAt(0);
		else
			sheet = workbook.getSheet(sheetName);

		if (null == sheet) {
			log.error("Sheet not exists");
			return null;
		}

		int rows = sheet.getPhysicalNumberOfRows();
		log.debug("total rows: {}", rows);

		final DataFormatter df = new DataFormatter();
		List<ExcelRow> excelRowList = new ArrayList<ExcelRow>();
		ExcelRow excelRow;

		for (int i = 0; i < rows && i < maxRow; ++i) {
			Row row = sheet.getRow(i);
			if (null != row) {
				excelRow = new ExcelRow();
				Cell cell;
				String value;
				for (int j = 0; j <= row.getLastCellNum(); ++j) {
					cell = row.getCell(j);
					if (null != cell) {
						value = df.formatCellValue(cell);
						log.debug("row: {}, cell: {}, value: {}", new Object[] { i, j, value });
						excelRow.add(j, value);
					}

				}
				excelRowList.add(excelRow);
			}

		}

		return excelRowList;
	}

	public void saveExcel(String fileFullPath, String sheetName, boolean isVersionAfter2007, boolean append, List<ExcelRow> header,
							List<ExcelRow> content) throws Exception {
		if (StringUtils.isEmpty(fileFullPath)) {
			log.error("File path is empty");
			return;
		}

		File file = new File(fileFullPath);

		boolean exists = file.exists();
		Workbook wb;
		if (exists && append) {
			wb = create(new FileInputStream(file), isVersionAfter2007, false, null);
		} else {
			if (isVersionAfter2007)
				wb = new XSSFWorkbook();
			else
				wb = new HSSFWorkbook();
		}

		OutputStream os = new FileOutputStream(file);
		boolean withHeader = false;

		if (StringUtils.isEmpty(sheetName))
			sheetName = "result";
		Sheet sheet = wb.getSheet(sheetName);
		if (null == sheet) {
			sheet = wb.createSheet(sheetName);
			withHeader = true;
		}

		Row row;
		int num = 0;

		/* Title. */
		if (withHeader) {
			if (null != header && !header.isEmpty()) {
				Map<Integer, String> cell2Value;
				Integer cell;
				String value;
				for (ExcelRow excelRow : header) {
					if (null != excelRow) {
						row = sheet.createRow(num);
						cell2Value = excelRow.getCell2Value();
						if (null != cell2Value) {
							for (Entry<Integer, String> entry : cell2Value.entrySet()) {
								cell = entry.getKey();
								value = entry.getValue();
								if (null != cell) {
									row.createCell(cell).setCellValue(value);
								}

							}
						}

						++num;
					}

				}

			}

		}

		/* Data. */
		if (exists && append) {
			num = sheet.getLastRowNum() + 1;
		}

		if (null != content && !content.isEmpty()) {
			Map<Integer, String> cell2Value;
			Integer cell;
			String value;
			for (ExcelRow excelRow : content) {
				if (null != excelRow) {
					row = sheet.createRow(num);
					cell2Value = excelRow.getCell2Value();
					if (null != cell2Value) {
						for (Entry<Integer, String> entry : cell2Value.entrySet()) {
							cell = entry.getKey();
							value = entry.getValue();
							if (null != cell) {
								row.createCell(cell).setCellValue(value);
							}

						}
					}

					++num;
				}

			}

		}

		os.flush();
		wb.write(os);
		os.close();

	}

	protected Workbook create(InputStream in, boolean isVersionAfter2007, boolean isEncrpty, String password)
			throws InvalidFormatException, IOException, GeneralSecurityException {
		Workbook workbook = null;

		if (isVersionAfter2007) {
			if (isEncrpty) {
				POIFSFileSystem pfs = new POIFSFileSystem(in);
				EncryptionInfo encInfo = new EncryptionInfo(pfs);
				Decryptor decryptor = Decryptor.getInstance(encInfo);

				if (decryptor.verifyPassword(password)) {
					in = decryptor.getDataStream(pfs);
				}

			}
			workbook = new XSSFWorkbook(OPCPackage.open(in));
			log.debug("excel version >= 2007");
		} else {
			if (isEncrpty) {
				Biff8EncryptionKey.setCurrentUserPassword(password);
			}

			workbook = new HSSFWorkbook(in);
			log.debug("excel version <= 2003");
		}

		return workbook;

	}

}
