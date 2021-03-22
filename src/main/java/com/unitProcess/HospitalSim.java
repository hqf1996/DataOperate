package com.unitProcess;

import java.util.List;

public class HospitalSim extends UnitSimAbstract{
	public HospitalSim(List<String[]> unitList, List<String[]> cityList) {
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
		return !(typeCode.equals("01") || typeCode.equals("02")|| typeCode.equals("05") || typeCode.equals("07"));
	}

	@Override
	String filterRegex() {
		// TODO Auto-generated method stub
		return ".*?(医院|疾病预防控制中心|疾控中心|血液中心|妇幼保健院)";
	}

}
