package com.sa.storm.sns.util;

import java.util.ArrayList;

import comp.sa.subtopic.nlp.IRLas;

public class TokenliztionUtil {

	ArrayList<String> list = IRLas.getInstance().doSegment("来测试下分词");
}
