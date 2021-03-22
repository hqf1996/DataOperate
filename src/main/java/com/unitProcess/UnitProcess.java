package com.unitProcess;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.util.Util;

public class UnitProcess {
	List<String[]> shortCallList;  //国内着名高校的简称，字段内容存放在UniversityShortCallStructure中
	UnitCleaner unitCleaner;  //通过匹配的方法来清洗单位
	UniversitySim universitySim;  //通过相似度匹配的方法找到相似度最高的学校
	InstitueSim institueSim;  //通过相似度匹配的方法找到相似度最高的研究所
	CompanySim companySim;  //通过相似度匹配的方法找到相似度最高的企业
	HospitalSim hospitalSim;
	GovernmentSim governmentSim;
	
	/**
	 * 构造方法
	 * @param unitList  标准单位列表，字段内容存放在UnitStructure中
	 * @param cityList  省市的列表，字段内容存放在UnitCityCodeMapStructure中
	 * @param shortCallList  国内着名高校的简称，字段内容存放在UniversityShortCallStructure中
	 */
	public UnitProcess(List<String[]> unitList, List<String[]> cityList, List<String[]> shortCallList) {
		// TODO Auto-generated constructor stub
		this(new UnitCleaner(unitList), new UniversitySim(unitList, cityList), new InstitueSim(unitList, cityList), 
				new CompanySim(unitList, cityList), new HospitalSim(unitList, cityList), new GovernmentSim(unitList, cityList),shortCallList);
	}
	public UnitProcess(UnitCleaner unitCleaner, UniversitySim universitySim, InstitueSim institueSim, CompanySim otherSim, HospitalSim hospitalSim, GovernmentSim governmentSim, List<String[]> shortCallList){
		this.shortCallList = shortCallList;
		this.unitCleaner = unitCleaner;
		this.universitySim = universitySim;
		this.institueSim = institueSim;
		this.companySim = otherSim;
		this.hospitalSim = hospitalSim;
		this.governmentSim = governmentSim;
	}
	public UnitInfo dispose(String unit){
		if(!unit.equals("null")){
			unit = UnitCleaner.pretreatmentData(unit);
		}
		UnitInfo unitInfo = unitCleaner.clean(unit);
		if(unitInfo != null && !unitInfo.getUnit().equals("null")){
//			unitInfo.setRankCode(unitInfo.getTypeCode() + "1");
			return unitInfo;
		}else{
			return unitClassify(unit);
		}
	}
	
	
	/*
	 * 判断字符串是否全是中文
	 */
	private boolean isChinese(String s){
		if(s == null || s.equals("")){
			return false;
		}
		Pattern pattern = Pattern.compile("[^\\u4E00-\\u9FBF]+");
		return !(pattern.matcher(s).find());
	}
	
	/**
	 * 为匹配上的单位dispose分类
	 * @param unit
	 * @return
	 */
	private UnitInfo unitClassify(String unit){
		Pattern schoolPattern = Pattern.compile(".*?(学校|学院|大学)");
		Pattern instituePattern = Pattern.compile(".*?(研究所|研究院|科学院|科院|研发中心|研究中心|实验室|设计院|科研中心)");
		Pattern companyPattern = Pattern.compile(".*?(企业|公司|集团|厂|银行)");
		Pattern hospitalPattern = Pattern.compile(".*?(医院|疾病预防控制中心|疾控中心|血液中心|妇幼保健院)");
		Matcher matcher;
		UnitInfo unitInfo;
		if((matcher = schoolPattern.matcher(unit)).find()){  //学校的处理
			if(matcher.group(0).contains("科学院")){
				unitInfo = FailUnitClean.institueFailClean(unit, institueSim);
			}else{
				unitInfo = FailUnitClean.universityFailClean(unit, shortCallList, universitySim);
			}
		}
		else if ((matcher = instituePattern.matcher(unit)).find()){  //研究所的处理
			unitInfo = FailUnitClean.institueFailClean(unit, institueSim);
		}
		else if ((matcher = companyPattern.matcher(unit)).find()) {  //企业的处理
			unitInfo = FailUnitClean.companyFailClean(unit, companySim);
		}else if ((matcher = hospitalPattern.matcher(unit)).find()) { //医院的处理
			unitInfo = FailUnitClean.hospitalFailClean(unit, hospitalSim);
		}else {  //党政机关的处理
			unitInfo = FailUnitClean.governmentFailClean(unit, governmentSim);
		}
		if(unitInfo.getUnit().equals("null")){
			unit = Util.getSplitFirst(unit, "！|!|;|/|、|,| ");
			if(isChinese(unit) && unit.length() > 4){
				unitInfo.setUnit(UnitCleaner.LastOpData(unit));
			}
		}
		return unitInfo;
	}
}
