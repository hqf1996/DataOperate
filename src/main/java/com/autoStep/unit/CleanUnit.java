package com.autoStep.unit;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CleanUnit{
	List<String[]> areainfos = null;
	List<String[]> cityinfos = null;
	List<String[]> provinceinfos = null;
	List<String[]> universityinfos = null;
	List<String[]> institueinfos = null;
	public CleanUnit(List<String[]> areainfos, List<String[]> cityinfos, 
			List<String[]> provinceinfos, List<String[]> universityinfos, List<String[]> institueinfos) {
		// TODO Auto-generated constructor stub
		this.areainfos = areainfos;
		this.cityinfos = cityinfos;
		this.provinceinfos = provinceinfos;
		this.universityinfos = universityinfos;
		this.institueinfos = institueinfos;
	}
	
	public UnitInfo clean(String firstOrganization){
		return clean("null", firstOrganization, "null");
	}
	
	public UnitInfo clean(String unit, String firstOrganization, String areacode){
		if(!unit.equals("null")){
			unit = unit.split("!|;|/")[0];
		}else {
			unit = firstOrganization.replaceAll("\\(.*?\\)", "").replaceAll("\\（.*?\\）", "");    //去掉括号里的内容
			Pattern pattern = Pattern.compile("[^\\d\\s()（）、,，.].*$");  //去掉前缀是数字空格括号的
			Matcher m = pattern.matcher(unit);
			if(!unit.equals("null")&&m.find()){  //m.find()必须要检验，否则会报错
				unit = m.group(0);
				unit = unit.split("!|;|/")[0];
			}
		}
		UnitInfo unitInfo = new UnitInfo();
		if(!unit.equals("null")){
			isInUniversityOrInstituesTable(unit, unitInfo);
			if(!unitInfo.getTypeCode().equals("null")){
				unitInfo.setUnitCode(unitToCode(unitInfo.getUnit()));
				return unitInfo;
			}
			isUniversityStr(unit, areacode, unitInfo);
			if(!unitInfo.getTypeCode().equals("null")){
				unitInfo.setUnitCode(unitToCode(unitInfo.getUnit()));
				return unitInfo;
			}
			isCompanyStr(unit, areacode, unitInfo);
			if(!unitInfo.getTypeCode().equals("null")){
				unitInfo.setUnitCode(unitToCode(unitInfo.getUnit()));
				return unitInfo;
			}
			isInstituesStr(unit, areacode, unitInfo);
			if(!unitInfo.getTypeCode().equals("null")){
				unitInfo.setUnitCode(unitToCode(unitInfo.getUnit()));
				return unitInfo;
			}
			unitInfo.setUnit(unit);
			unitInfo.setProvinceCode(getProvinceFromUnit(unit, areacode));
			unitInfo.setTypeCode(Message.OTHERUNIT);
			unitInfo.setRankCode(Message.OTHER);
			unitInfo.setUnitCode(unitToCode(unitInfo.getUnit()));
			return unitInfo;
		}else {
			unitInfo.setTypeCode(Message.OTHERUNIT);
			unitInfo.setRankCode(Message.OTHER);
			unitInfo.setUnitCode(unitToCode("99999999"));
			return unitInfo;
		}
	}
	
	
	/**
	 * 判断unit是否在着名大学或研究院表中
	 * @param unit
	 */
	private void isInUniversityOrInstituesTable(String unit,UnitInfo infos){
		/*
		 * 首先判断是否在大学里
		 */
/*			for(String[] strs:universityinfos){
			for(String str:strs){
				System.out.print(str+" ");
			}
			System.out.println("");
		}*/
		for(String[] strs :universityinfos){
			if(unit.equals(strs[3])){
				infos.setUnit(unit);
				infos.setCity(strs[2]);
				infos.setProvinceCode(getCodefromUnit(strs[1]));
				infos.setTypeCode(Message.UNIVERSUTY);
				infos.setRankCode(Message.GOODUNIVERSITY);
				return;
			}
		}
		for(String[] strs :universityinfos){
			if(unit.indexOf(strs[3]) != -1){
				infos.setUnit(strs[3]);
				infos.setCity(strs[2]);
				infos.setProvinceCode(getCodefromUnit(strs[1]));
				infos.setTypeCode(Message.UNIVERSUTY);
				infos.setRankCode(Message.GOODUNIVERSITY);
				return;
			}
		}
		
		/*
		 *接着判断是否在研究所中 
		 */
		for(String[] strs :institueinfos){
			if(unit.equals(strs[3])){
				infos.setUnit(unit);
				infos.setCity(strs[2]);
				infos.setProvinceCode(getCodefromUnit(strs[1]));
				infos.setTypeCode(Message.INSTITUES);
				infos.setRankCode(Message.GOODINSTITUES);
				return;
			}
		}
		for(String[] strs :institueinfos){
			if(unit.startsWith(strs[3])){
				infos.setUnit(strs[3]);
				infos.setCity(strs[2]);
				infos.setProvinceCode(getCodefromUnit(strs[1]));
				infos.setTypeCode(Message.INSTITUES);
				infos.setRankCode(Message.GOODINSTITUES);
				return;
			}
		}
	}
	
	/**
	 * 字段匹配是否是大学
	 * @param unit
	 * @param areacode
	 * @param infos
	 */
	private void isUniversityStr(String unit,String areacode,UnitInfo infos){
		Pattern pattern = Pattern.compile(".*?(学校|学院|大学)");
		Matcher matcher = pattern.matcher(unit);
		if(matcher.find()){
			unit = matcher.group(0);
			if(!unit.contains("科学院")){
				infos.setUnit(unit);
				infos.setProvinceCode(getProvinceFromUnit(unit, areacode));
				infos.setTypeCode(Message.UNIVERSUTY);
				infos.setRankCode(Message.COMMONUNIVERSITY);
			}
		}
	}
	
	/**
	 * 字段匹配是否为研究所
	 * @param unit
	 * @param areacode
	 * @param infos
	 */
	private void isInstituesStr(String unit,String areacode,UnitInfo infos){
		Pattern pattern = Pattern.compile(".*?(研究所|研究院|科学院|科院|研究中心)");
		Matcher matcher = pattern.matcher(unit);
		if(matcher.find()){
			unit = matcher.group(0);
			infos.setUnit(unit);
			infos.setProvinceCode(getProvinceFromUnit(unit, areacode));
			infos.setTypeCode(Message.INSTITUES);
			infos.setRankCode(Message.COMMONINSTITUES);
		}
	}
	
	/**
	 * 字段匹配是否为公司
	 * @param unit
	 * @param areacode
	 * @param infos
	 */
	private void isCompanyStr(String unit,String areacode,UnitInfo infos){
		unit = unit.replaceAll("\\(.*?\\)", ""); 
		Pattern pattern = Pattern.compile(".*?(公司|集团|医院|厂|场)");
		Matcher matcher = pattern.matcher(unit);
		if(matcher.find()){
			unit = matcher.group(0);
			infos.setUnit(unit);
			infos.setProvinceCode(getProvinceFromUnit(unit, areacode));
			infos.setTypeCode(Message.COMPANY);
			infos.setRankCode(Message.COMMONCOMPANY);
		}
	}
	
	/**
	 * 从unit和areacode中获得中国省份的areacode
	 * @param unit
	 * @param areacode
	 * @return
	 */
	private String getProvinceFromUnit(String unit,String areacode){
		if(areacode.equals("null")){
			//在unit_areacode表中过滤一遍
			for(String[] strs: areainfos){
				if(unit.startsWith(strs[1])){
					areacode = strs[2];
//					System.out.println("----->>>>>>"+strs[1]);
					//判断是否是中国的省份
					return isProvinceCode(areacode);
				}
			}
			//areacode没有在unit_areacode中匹配到
			return getCodefromUnit(unit);
		}else{
			//判断是否为中国的省份
			return isProvinceCode(areacode);
		}
	}
	
	/**
	 * 根据areacode判断是否为在中国省份内
	 * @param areacode
	 * @return
	 */
	private String isProvinceCode(String areacode){
		for(String[] strs : provinceinfos){
			if(areacode.equals(strs[2])){
				return areacode;
			}
		}
		return "99";
	}
	
	/**
	 * 根据单位或省份的字段匹配来得到省份的code
	 * @param unit
	 * @return 从省份字段匹配出来的省份code
	 */
	private String getCodefromUnit(String province){
		for(String[] strs : cityinfos){
			if(province.contains(strs[1])){
				return strs[2];
			}
		}
		return "99";
	}
	
	private String unitToCode(String unit){
		for(String[] areainfo:areainfos){
			if(unit.startsWith(areainfo[1])){
				return String.format("%08d", Integer.parseInt(areainfo[0]));
			}
		}
		return "99999999";
	}
	
}
