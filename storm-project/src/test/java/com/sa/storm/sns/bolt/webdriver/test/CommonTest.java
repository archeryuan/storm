package com.sa.storm.sns.bolt.webdriver.test;


public class CommonTest {

	/**
	 * @description
	 * 
	 * @param
	 * @return
	 * @throws Exception
	 * @throws
	 * 
	 * @author Luke
	 * @created date 2014-12-10
	 * @modification history<BR>
	 *               No. Date Modified By <BR>
	 *               Why & What</BR> is modified
	 * 
	 * @see
	 */
	public static void main(String[] args) throws Exception {
		/*
		 * List<KeyValuePair> list = new ArrayList<KeyValuePair>(); KeyValuePair k1 = new KeyValuePair(); k1.setKey("01"); k1.setValue(0L);
		 * list.add(k1);
		 * 
		 * k1 = new KeyValuePair(); k1.setKey("02"); k1.setValue(10L); list.add(k1);
		 * 
		 * k1 = new KeyValuePair(); k1.setKey("03"); k1.setValue(20L); list.add(k1);
		 * 
		 * k1 = new KeyValuePair(); k1.setKey("04"); k1.setValue(0L); list.add(k1);
		 * 
		 * for (KeyValuePair k : list) System.out.println(k);
		 * 
		 * KeyValuePairComparator keyValuePairComparator = new KeyValuePairComparator(); Collections.sort(list, keyValuePairComparator);
		 * 
		 * System.out.println("---------------");
		 * 
		 * for (KeyValuePair k : list) System.out.println(k);
		 */

		/*
		 * List<TopCategory> topCategoryList = new ArrayList<TopCategory>(); List<TopLikePage> topLikePageList = new
		 * ArrayList<TopLikePage>();
		 * 
		 * TopCategory topCategory = new TopCategory(); topCategory.setCategoryName("tt"); topCategory.setInterestFans(10);
		 * 
		 * topCategoryList.add(topCategory);
		 * 
		 * topCategory = new TopCategory(); topCategory.setCategoryName("ss"); topCategory.setInterestFans(20);
		 * 
		 * topCategoryList.add(topCategory);
		 * 
		 * 
		 * TopLikePage page = new TopLikePage(); ImgInfo imgInfo = new ImgInfo();
		 * 
		 * imgInfo.setLargeImgUrl("l.png"); imgInfo.setSmallImgUrl("s.png");
		 * 
		 * List<String> shareImgUrls = new ArrayList<String>(); shareImgUrls.add("a.png"); shareImgUrls.add("b.png");
		 * shareImgUrls.add("c.png");
		 * 
		 * imgInfo.setShareImgUrls(shareImgUrls);
		 * 
		 * 
		 * page.setCategory("01"); page.setPageName("test"); page.setInterestFans(100L); page.setImgInfo(imgInfo);
		 * 
		 * topLikePageList.add(page);
		 * 
		 * page = new TopLikePage(); imgInfo = new ImgInfo();
		 * 
		 * imgInfo.setLargeImgUrl("l2.png"); imgInfo.setSmallImgUrl("s2.png");
		 * 
		 * shareImgUrls = new ArrayList<String>(); shareImgUrls.add("a2.png"); shareImgUrls.add("b2.png"); shareImgUrls.add("c2.png");
		 * 
		 * imgInfo.setShareImgUrls(shareImgUrls);
		 * 
		 * 
		 * page.setCategory("02"); page.setPageName("test2"); page.setInterestFans(100L); page.setImgInfo(imgInfo);
		 * 
		 * topLikePageList.add(page);
		 * 
		 * Map<String, Object> map = new HashMap<String, Object>(); map.put("categoryList", topCategoryList); //map.put("pageLikeList",
		 * topLikePageList);
		 * 
		 * String json = JsonUtil.getMapper().writeValueAsString(topCategoryList); System.out.println("---------------");
		 * System.out.println(json);
		 * 
		 * List<?> list = JsonUtil.getMapper().readValue(json, List.class);
		 */

		/* Clear useless redis key. */
		/*
		 * RedisUtil redisUtil = RedisUtil.getInstance(); String date = "2014-12-12"; String myPageId = "123313947709384"; String pattern =
		 * date + "_" + myPageId + "*"; Set<String> keys = redisUtil.keys(pattern); redisUtil.del(keys.toArray(new String[keys.size()]));
		 * 
		 * System.out.println("done");
		 */

		/*
		 * SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS"); Calendar cal = Calendar.getInstance(); String date =
		 * df.format(cal.getTime());
		 * 
		 * System.out.println(date);
		 */

		/*
		 * String str = "KUMARIi Japanese Language School(クマリ日本語学校)"; str = URLEncoder.encode(str, "utf-8"); System.out.println("str==" +
		 * str); String after =
		 * URLDecoder.decode("KUMARIi-Japanese-Language-School%E3%82%AF%E3%83%9E%E3%83%AA%E6%97%A5%E6%9C%AC%E8%AA%9E%E5%AD%A6%E6%A0%A1",
		 * "utf-8"); System.out.println("after==" + after);
		 */

		/*
		 * String[] strs = {"ab", "$$", "$$$$", "$$$a", "48", "abc66648", "55tt66"}; for (String str : strs) { boolean b =
		 * Pattern.matches("(\\w*\\$+\\w*)|(^\\d+$)", str); System.out.println(str + ": " + b); }
		 */

		/*
		 * List<String> lines = new ArrayList<String>(); byte[] bom = {(byte)0xEF, (byte)0xBB, (byte)0xBF}; lines.add(new String(bom));
		 * lines.add("test, 10"); lines.add("香港, 20"); File file = new File("test.csv"); FileUtils.writeLines(file, "utf-8", lines, "\n");
		 */

		/*
		 * Map<String, Long> category2Count = new HashMap<String, Long>(); category2Count.put("a1", 10l); category2Count.put("a2", 20l);
		 * List<Map.Entry<String, Long>> list = null; if (null != category2Count) { list = new ArrayList<Map.Entry<String,
		 * Long>>(category2Count.entrySet()); Collections.sort(list, new Comparator<Map.Entry<String, Long>>() { public int
		 * compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) { long v1 = o1.getValue(); long v2 = o2.getValue(); return v2 >
		 * v1 ? 1 : 0; } }); } for (Map.Entry<String, Long> map : list) { System.out.println(map.getKey() + ":" + map.getValue()); }
		 */

	
		String str2Unicode = string2Unicode("@");
		System.out.println("str2Unicode: " + str2Unicode);		

		String unicode2Str = unicode2String(str2Unicode);//StringEscapeUtils.unescapeJava(str2Unicode);
		System.out.println("unicode2Str: " + unicode2Str);

		
		System.out.println("done!");

	}

	
	public static String string2Unicode(String normalString) {

	    String hexCodeWithLeadingZeros = "";
	    try {
	      for (int index = 0; index < normalString.length(); index++) {
	        String hexCode = Integer.toHexString(normalString.codePointAt(index)).toUpperCase();
	        String hexCodeWithAllLeadingZeros = "0000" + hexCode;
	        String temp = hexCodeWithAllLeadingZeros.substring(hexCodeWithAllLeadingZeros.length() - 4);
	        hexCodeWithLeadingZeros += ("\\u" + temp);
	         }
	          return hexCodeWithLeadingZeros;
	       } catch (NullPointerException nlpException) {
	         return hexCodeWithLeadingZeros;
	          }
	    }


	public static String unicode2String(String unicodeString) {
	    String str = unicodeString.split(" ")[0];
	    str = str.replace("\\", "");
	    String[] arr = str.split("u");
	    String text = "";
	    for (int i = 1; i < arr.length; i++) {
	      int hexVal = Integer.parseInt(arr[i], 16);
	      text += (char) hexVal;
	    }
	    return text;
	  }

	


}
