package com.unitProcess;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.structure.UniversityShortCallStructure;
import com.util.Util;

public class FailUnitClean {
	public static UnitInfo universityFailClean(String unit, List<String[]> shortCallList, UniversitySim universitySim){
		UnitInfo unitInfo = new UnitInfo();
		String[] newUnitsInfos = universitySim.similarity(unit);
		if(Double.valueOf(newUnitsInfos[newUnitsInfos.length-1]) >= 0.86){
			UnitCleaner.writeInfos(unitInfo, newUnitsInfos);
//			unitInfo.setRankCode(unitInfo.getTypeCode() + "1");
		}else {
			//大学简写匹配
			for(String[] shortCallInfos:shortCallList){
				if(unit.startsWith(shortCallInfos[UniversityShortCallStructure.unitShortName])){
					UnitCleaner.writeInfos(unitInfo, shortCallInfos);
//					unitInfo.setRankCode(unitInfo.getTypeCode() + "1");
					return unitInfo;
				}
			}
			//匹配不上，直接取出大学字段
			String areaCode = universitySim.getUnitAreaCode(unit);
			if(areaCode != null){
				unitInfo.setProvinceCode(areaCode);
			}
			unit = Util.getSplitFirst(unit.replaceAll("^\\d+", ""), "！|!|;|/|、|,");
			Pattern schoolPattern = Pattern.compile(".*?(学校|学院|大学)");
				Matcher matcher;
		        if( (matcher=schoolPattern.matcher(unit)).find()){
		        	unit = matcher.group(0);
		        	unit = unit.replaceAll("(?<=[\\u4E00-\\u9FBF])[\\s|.|。|]+(?=[\\u4E00-\\u9FBF])", "");
		        	if(unit.length() >= 4){
		        		unitInfo.setUnit(UnitCleaner.LastOpData(unit));
		    			unitInfo.setTypeCode(Message.UNIVERSUTY);
		    			unitInfo.setRankCode(Message.COMMONUNIVERSITY);
		        	}else {
						unitInfo.setTypeCode("99");
					}
		        }else {
		        	unitInfo.setTypeCode("99");
				}   
		}
		return unitInfo;
	}
	
	public static UnitInfo institueFailClean(String unit, InstitueSim institueSim){
		UnitInfo unitInfo = new UnitInfo();
		String[] newUnitsInfos = institueSim.similarity(unit);
		if(Double.valueOf(newUnitsInfos[newUnitsInfos.length -1]) >= 0.86){
			UnitCleaner.writeInfos(unitInfo, newUnitsInfos);
//			unitInfo.setRankCode(unitInfo.getTypeCode() + "1");
		}else {
			//匹配不上，直接取出研究所字段
			String areaCode = institueSim.getUnitAreaCode(unit);
			if(areaCode != null){
				unitInfo.setProvinceCode(areaCode);
			}
			unit = Util.getSplitFirst(unit.replaceAll("^([（）\\d\\s１２３４５６７８９０oOl]*(\\.|．)([\\d１２３４５６７８９０]{6})*)|^([\\d１２３４５６７８９０oOl]{6})*", "").replaceAll("^\\w(?=[\\u4E00-\\u9FBF])", ""), "！|!|;|/|、|,");
			Pattern instituePattern = Pattern.compile(".*?实验室|.*?(研发中心|研究中心|科研中心)|.*?研究所|.*?(研究院|科学院|科院|设计院)");
			Matcher matcher;
			if((matcher = instituePattern.matcher(unit)).find()){
				unit = matcher.group(0);
				unit = unit.replaceAll("(?<=[\\u4E00-\\u9FBF])[\\s|.|。|]+(?=[\\u4E00-\\u9FBF])", "");
				if(unit.length() >= 5){
					unitInfo.setUnit(UnitCleaner.LastOpData(unit));
					unitInfo.setTypeCode(Message.INSTITUES);
					unitInfo.setRankCode(Message.COMMONINSTITUES);
				}else {
					unitInfo.setTypeCode("99");
				}
			}else {
				unitInfo.setTypeCode("99");
			}
		}
		return unitInfo;
	}
	
	public static UnitInfo companyFailClean(String unit, CompanySim companySim){
		UnitInfo unitInfo = new UnitInfo();
//		String[] newUnitsInfos = companySim.similarity(unit);
//		if(Double.valueOf(newUnitsInfos[newUnitsInfos.length -1]) >= 0.86){
//			UnitCleaner.writeInfos(unitInfo, newUnitsInfos);
//			unitInfo.setRankCode(unitInfo.getTypeCode() + "1");
//		}else {
			//匹配不上，直接取出公司字段
			String areaCode = companySim.getUnitAreaCode(unit);
			if(areaCode != null){
				unitInfo.setProvinceCode(areaCode);
			}
			unit = Util.getSplitFirst(unit.replaceAll("^([（）\\d\\s１２３４５６７８９０oOl]*(\\.|．)([\\d１２３４５６７８９０]{6})*)|^([\\d１２３４５６７８９０oOl]{6})*", "").replaceAll("（.*?[\\d１２３４５６７８９０oOl]{6}.*?）", "").replaceAll("^\\w(?=[\\u4E00-\\u9FBF])", ""), "！|!|;|/|、|,");
			Pattern companyPattern = Pattern.compile(".*?(公司|银行)|.*?集团|。*?企业|.*?厂");
			Matcher matcher;
			if((matcher = companyPattern.matcher(unit)).find()){
				unit = matcher.group(0);
				unit = unit.replaceAll("(?<=[\\u4E00-\\u9FBF])[\\s|.|。|]+(?=[\\u4E00-\\u9FBF])", "");
				if(unit.length() >= 4){
					unitInfo.setUnit(UnitCleaner.LastOpData(unit));
					unitInfo.setTypeCode(Message.COMPANY);
					unitInfo.setRankCode(Message.COMMONCOMPANY);
				}else {
					unitInfo.setTypeCode("99");
				}
			}else {
				unitInfo.setTypeCode("99");
			}
//		}
		return unitInfo;
	}
	
	public static UnitInfo hospitalFailClean(String unit, HospitalSim hospitalSim){
		UnitInfo unitInfo = new UnitInfo();
		String[] newUnitsInfos = hospitalSim.similarity(unit);
		if(Double.valueOf(newUnitsInfos[newUnitsInfos.length -1]) >= 0.86){
			UnitCleaner.writeInfos(unitInfo, newUnitsInfos);
//			unitInfo.setRankCode(unitInfo.getTypeCode() + "1");
		}else {
			//匹配不上，直接取出医院字段
			String areaCode = hospitalSim.getUnitAreaCode(unit);
			if(areaCode != null){
				unitInfo.setProvinceCode(areaCode);
			}
			unit = Util.getSplitFirst(unit.replaceAll("^([（）\\d\\s１２３４５６７８９０oOl]*(\\.|．)([\\d１２３４５６７８９０]{6})*)|^([\\d１２３４５６７８９０oOl]{6})*", "").replaceAll("（.*?[\\d１２３４５６７８９０oOl]{6}.*?）", "").replaceAll("^\\w(?=[\\u4E00-\\u9FBF])", ""), "！|!|;|/|、|,");
			Pattern companyPattern = Pattern.compile(".*?(医院|疾病预防控制中心|疾控中心|血液中心|妇幼保健院)");
			Matcher matcher;
			if((matcher = companyPattern.matcher(unit)).find()){
				unit = matcher.group(0);
				unit = unit.replaceAll("(?<=[\\u4E00-\\u9FBF])[\\s|.|。|]+(?=[\\u4E00-\\u9FBF])", "");
				if(unit.length() >= 4){
					unitInfo.setUnit(UnitCleaner.LastOpData(unit).replace("疾控中心", "疾病预防控制中心"));
					unitInfo.setTypeCode(Message.HOSPITAL);
					unitInfo.setRankCode(Message.COMMONHOSPITAL);
				}else {
					unitInfo.setTypeCode("99");
				}
			}else {
				unitInfo.setTypeCode("99");
			}
		}
		return unitInfo;
	}
	
	public static UnitInfo governmentFailClean(String unit, GovernmentSim governmentSim){
		UnitInfo unitInfo = new UnitInfo();
		String[] newUnitsInfos = governmentSim.similarity(unit);
		if(Double.valueOf(newUnitsInfos[newUnitsInfos.length -1]) >= 0.86){
			UnitCleaner.writeInfos(unitInfo, newUnitsInfos);
//			unitInfo.setRankCode(unitInfo.getTypeCode() + "1");
		}else {
			//匹配不上，直接取出党政机关字段
			String areaCode = governmentSim.getUnitAreaCode(unit);
			if(areaCode != null){
				unitInfo.setProvinceCode(areaCode);
			}
			unit = Util.getSplitFirst(unit.replaceAll("^([（）\\d\\s１２３４５６７８９０oOl]*(\\.|．)([\\d１２３４５６７８９０]{6})*)|^([\\d１２３４５６７８９０oOl]{6})*", "").replaceAll("（.*?[\\d１２３４５６７８９０oOl]{6}.*?）", "").replaceAll("^\\w(?=[\\u4E00-\\u9FBF])", ""), "！|!|;|/|、|,| ");
			Pattern companyPattern = Pattern.compile(".*?(厅|局|党委|政府|知识产权中心|检察院|法院|司法鉴定中心|计划生育委员会|发展和改革委员会|监督管理委员会|基金委员会|经济和信息化委员会|科学技术工业委员会)");
			Matcher matcher;
			if((matcher = companyPattern.matcher(unit)).find()){
				unit = matcher.group(0);
				unit = unit.replaceAll("(?<=[\\u4E00-\\u9FBF])[\\s|.|。|]+(?=[\\u4E00-\\u9FBF])", "");
				if(unit.length() >= 4 && unit.charAt(1) != '省' && !unit.contains("《") && !unit.contains("》")){
					unitInfo.setUnit(UnitCleaner.LastOpData(unit));
					unitInfo.setTypeCode(Message.GOVERNMENT);
					unitInfo.setRankCode(Message.COMMONGOVERNMENT);
				}
				else {
					unitInfo.setTypeCode("99");
				}
			}
			else {
				unitInfo.setTypeCode("99");
			}
		}
		return unitInfo;
	}
}
