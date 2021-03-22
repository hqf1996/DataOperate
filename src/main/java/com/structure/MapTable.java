package com.structure;

import java.util.HashMap;
import java.util.Map;

public class MapTable {
	public static final Map<String, String> subjectCodeMap = new HashMap<String, String>();
	static{
		subjectCodeMap.put("电子信息", "A1");
		subjectCodeMap.put("新能源与节能", "A2");
		subjectCodeMap.put("航空航天", "A3");
		subjectCodeMap.put("新材料", "A4");
		subjectCodeMap.put("化学化工", "A5");
		subjectCodeMap.put("机械电子与制造", "A9");
		subjectCodeMap.put("生物医药", "C1");
		subjectCodeMap.put("资源与环境", "C2");
		subjectCodeMap.put("农林牧渔", "Z1");
		subjectCodeMap.put("其他", "Z9");
	}
}
