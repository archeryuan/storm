package com.sa.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.sa.storm.sns.util.ExcelUtil;
import com.sa.storm.sns.util.ExcelUtil.ExcelRow;

public class ExcelUtilUnitTest {

	//@Test
	public void testSaveExcel() throws Exception {
		ExcelUtil util = ExcelUtil.getInstance();

		/*Header rows.*/
		List<ExcelRow> header = new ArrayList<ExcelRow>();
		Map<Integer, String> cell2Value = new HashMap<Integer, String>();
		cell2Value.put(0, "result as following");
		ExcelRow excelRow = new ExcelRow();
		excelRow.setCell2Value(cell2Value);
		header.add(excelRow);

		excelRow = new ExcelRow();
		cell2Value = new HashMap<Integer, String>();
		cell2Value.put(0, "name");
		cell2Value.put(1, "email");
		cell2Value.put(2, "phone");
		cell2Value.put(3, "address");
		excelRow.setCell2Value(cell2Value);
		header.add(excelRow);

		/*Content rows.*/
		List<ExcelRow> content = new ArrayList<ExcelRow>();
		for (int i = 0; i < 3; ++i) {
			excelRow = new ExcelRow();
			excelRow.add(0, "姓名" + i);
			excelRow.add(1, "email" + i);
			excelRow.add(2, "phone" + i);
			excelRow.add(3, "address" + i);
			content.add(excelRow);
		}

		util.saveExcel("D:\\2003.xls", "test", false, true, header, content);
		util.saveExcel("D:\\2003.xls", "test1", false, true, header, content);
		/*util.saveExcel("D:\\2007.xlsx", "test", true, true, header, content);
		util.saveExcel("D:\\2007.xlsx", "test1", true, true, header, content);*/
		System.out.println("done");
	}
	
	@Test
	public void testLoadExcel() throws Exception {
		ExcelUtil util = ExcelUtil.getInstance();
		List<ExcelRow> excelRowList = util.load("D:\\2003.xls", "test", 3);
		
		if (null != excelRowList) {
			for (ExcelRow excelRow : excelRowList) {
				System.out.println(excelRow);
			}
		}
		
		System.out.println("done");
	}


}
