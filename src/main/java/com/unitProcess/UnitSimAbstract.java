package com.unitProcess;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;

public abstract class UnitSimAbstract implements SimilarityFun{
	List<String[]> unitList;
	List<String[]> cityList;
	UnitInfo unitInfo;
	public String[] similarity(String unit){
		double maxCos = 0;
		String[] newUnisInfos = new String[]{"", "", "99", "99999999", "01"};
		String areaCode = getUnitAreaCode(unit);
		Matcher matcher;
		for (int i = 0; i < 2 && unit.length() > 3; i++) {//循环两遍，一遍是整个字段算相似度，一个是取大学等字段前的字符串
			for (String[] infos : unitList) {
				if ((areaCode == null || infos[UnitStructure.areaCode].equals(areaCode))
						&& isRightTypeCode(infos[UnitStructure.typeCode])) {
					double cos = run(unit, infos[UnitStructure.unit]);
					/*if (cos == maxCos) {
						newUnit = newUnit + " " + infos[UnitStructure.unit];
					}*/
					if (cos > maxCos) {
						maxCos = cos;
						newUnisInfos = infos;
					}
				}
			}
			
			if ((matcher = Pattern.compile(filterRegex()).matcher(unit)).find()) {
				unit = matcher.group(0);
			} else {
				break;
			}
		}
		newUnisInfos = Arrays.copyOf(newUnisInfos, newUnisInfos.length + 1);
		newUnisInfos[newUnisInfos.length - 1] = Double.toString(maxCos); 
		return newUnisInfos;
	}
	
	/**
	 * 获得单位的省份信息，如果包含，返回省份代码，否则返回null
	 * @return
	 */
	public String getUnitAreaCode(String unit){
		for(String[] infos: cityList){
			if(unit.contains(infos[UnitCityCodeMapStructure.unit])){
				return infos[UnitCityCodeMapStructure.areaCode];
			}
		}
		return null;
	}
	
	abstract boolean isRightTypeCode(String typeCode);
	/**
	 * (infos[UnitStructure.typeCode].equals("01") || infos[UnitStructure.typeCode].equals("05")
								|| infos[UnitStructure.typeCode].equals("06")
	 * @param areaCode
	 * @return
	 */
	
	abstract String filterRegex();
}
