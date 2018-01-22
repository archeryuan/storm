package com.sa.util;



public class TokenizationTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String sp = "有背光的机械式键盘hello world";
		String tra = "有背光的機械式鍵盤hello world";
		
		System.out.println("sp: " + containTraditionalChinese(sp));
		System.out.println("tra: " + containTraditionalChinese(tra));
		System.out.println("--------------------");
		
		/*System.out.println(ZHConverter.convert(tra, ZHConverter.SIMPLIFIED));
		System.out.println(ZHConverter.convert(sp, ZHConverter.TRADITIONAL));
		
		System.out.println("--------------------");
		ChineseJF tradSimpConvertor  = CJFBeanFactory.getChineseJF();
		System.out.println(tradSimpConvertor.chineseFan2Jan(tra));
		System.out.println(tradSimpConvertor.chineseJan2Fan(sp));
		
		String title = null;
		String content = "有俄罗斯国会议员，9号在社交网站推特表示，美国中情局前雇员斯诺登，已经接受委内瑞拉的庇护，不过推文在发布几分钟后随即删除。俄罗斯当局拒绝发表评论，而一直协助斯诺登的维基解密否认他将投靠委内瑞拉。　　俄罗斯国会国际事务委员会主席普什科夫，在个人推特率先披露斯";
		KeyWordComputer kwc = new KeyWordComputer(5);
		Collection<Keyword> keywordList = kwc.computeArticleTfidf(title, content);
		System.out.println(keywordList);*/
		
		
	}
	
	
	public static boolean containTraditionalChinese(String str) {
		StringBuffer ss=new StringBuffer(str);
		
		for (int i = 0; i < ss.length(); ++i) {
			if ( (int)ss.charAt(i) >= 0x4e00 && (int)ss.charAt(i) <= 0x9fa5 )
				return true;
		}
		
		return false;
		
	}

}
