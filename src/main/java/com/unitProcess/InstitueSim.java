package com.unitProcess;

import java.util.List;

public class InstitueSim extends UnitSimAbstract{
	public InstitueSim(List<String[]> unitList, List<String[]> cityList) {
		// TODO Auto-generated constructor stub
		this.unitList = unitList;
		this.cityList = cityList;
	}
	@Override
	public double run(String source, String target) {
		// TODO Auto-generated method stub
		return StringSimilarity.cosineSimilarity(source, target);
	}

	@Override
	boolean isRightTypeCode(String typeCode) {
		// TODO Auto-generated method stub
		return typeCode.equals("02") || typeCode.equals("06")|| typeCode.equals("07");
	}

	@Override
	String filterRegex() {
		// TODO Auto-generated method stub
		return ".*?(研究所|研究院|科学院|科院|中心|实验室|设计院)";
	}

}
