package util;

import java.util.ArrayList;

import comp.sa.subtopic.nlp.IRLas;

public class TestNLPIR {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String input = "来测试下分词 Local media reports cite Mullah's lawyers saying they will petition for this harsher sentence to be reviewed, but the attorney general has said there can be no appeal against a Supreme Court verdict";
		ArrayList<String> list = IRLas.getInstance().doSegment(input);
		System.out.println("begin "+ list.size());
		for(String str : list)
			System.out.println(str);
	}

}
