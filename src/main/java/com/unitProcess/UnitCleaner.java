package com.unitProcess;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.structure.UnitStructure;
import com.util.Util;

public class UnitCleaner{ //通过匹配来清洗单位
	List<String[]> unitList = null;
	List<SpecialUnit> specialUnitList = new ArrayList<SpecialUnit>();
	Comparator<String> preCp;
	Comparator<String> allCp;
	
	class SpecialUnit{
		String[] infos;
		String unit;
		String province;
		public SpecialUnit(String[] infos, String unit, String province) {
			// TODO Auto-generated constructor stub
			this.infos = infos;
			this.unit = unit;
			this.province = province;
		}
		public String[] getInfos() {
			return infos;
		}
		public void setP(String[] infos) {
			this.infos = infos;
		}
		public String getUnit() {
			return unit;
		}
		public void setUnit(String unit) {
			this.unit = unit;
		}
		public String getProvince() {
			return province;
		}
		public void setProvince(String province) {
			this.province = province;
		}
	}
	
	UnitCleaner(List<String[]> unitList){
		if(unitList.size() <= 0 || unitList.get(0).length < 5){
			throw new IllegalArgumentException("unitInfos args is too low");
		}
		this.unitList = unitList;
		this.preCp = new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				// TODO Auto-generated method stub
				return s2.startsWith(s1)?1:-1;
			}
		};
		this.allCp = new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				// TODO Auto-generated method stub
				return s2.indexOf(s1)==-1?-1:1;
			}
		};
		Pattern pattern = Pattern.compile("(?<=（).*(?=）)");
		Matcher matcher;
		//带括号的标准字段需要特殊处理
		for(String[] infos: unitList){
			if((matcher = pattern.matcher(infos[UnitStructure.unit])).find()){
				specialUnitList.add(new SpecialUnit(infos, infos[UnitStructure.unit].replaceAll("\\（.*?\\）", ""), matcher.group(0)));
			}
		}
	}
	
	public UnitInfo clean(String unit) {
		// TODO Auto-generated method stub
		return this.clean(unit, null, "99");
	}

	public UnitInfo clean(String unit, String areacode) {
		// TODO Auto-generated method stub
		return this.clean(unit, null, areacode);
	}
	
	public UnitInfo clean(String unit, String firstOrganization, String areacode) {
		// TODO Auto-generated method stub
		UnitInfo unitInfo = new UnitInfo();
		unitInfo.setTypeCode(Message.OTHERUNIT);
		unitInfo.setRankCode(Message.OTHER);
		unitInfo.setUnitCode("99999999");
		if(firstOrganization != null && !firstOrganization.equals("null")){
			unit = firstOrganization;
		}
		if(unit != null && unit.equals("null")){
			return unitInfo;
			
		}
		unit = unit.replaceAll("[\\s.]", "");
		String unitFirst = Util.getSplitFirst(unit, "！|!|;|/|、|,");
		String DUnit = unit.replaceAll("\\（.*?\\）", "");
		String[] newUnitInfos = null;
		//带括号的标准单位特殊处理
		for(SpecialUnit sUnit: specialUnitList){
			if((unit.contains(sUnit.getUnit()) || DUnit.contains(sUnit.getUnit())) && unit.contains(sUnit.getProvince())){
				writeInfos(unitInfo, sUnit.getInfos());
				return unitInfo;
			}
		}
		for(int i= 0;i<2;i++){
			if ((newUnitInfos = compareStr(unitFirst, preCp)) != null){
				writeInfos(unitInfo, newUnitInfos);
				return unitInfo;
			};
			if((newUnitInfos = compareStr(unit, allCp)) != null){
				writeInfos(unitInfo, newUnitInfos);
				return unitInfo;
			}
			unit = DUnit;
			unitFirst = unitFirst.replace("\\（.*?\\）", "");
		}
		return unitInfo;
	}
	
	/**
	 * 单位匹配之前的预处理
	 * @param s
	 * @return
	 */
	public static String pretreatmentData(String s){
		s = s.replace("(", "（").replace(")","）");
		return delSpecialChar(s);
	}
	
	/**
	 * 单位最后一步处理
	 * 处理括号不匹配和全角字符
	 */
	public static String LastOpData(String s){
		if (s.isEmpty()) {  
            return s;  
        }  
		int leftCount = 0, rightCount = 0;  
        char[] charArray = s.toCharArray();  
        for (int i = 0; i < charArray.length; i++) {  
            if (charArray[i] == 12288) {  
                charArray[i] =' ';  
            }else if (s.charAt(i) == '（') {
				leftCount ++;
				charArray[i] ='（';
			}else if (s.charAt(i) == '）') {
				rightCount ++;
				charArray[i] ='）';
			}else if (charArray[i] >= '\uFF00' &&  
                    charArray[i]  <= '\uFF5F') {  
                charArray[i] = (char) (charArray[i] - 65248);  
            }
        }
        String result = new String(charArray);
        if((leftCount * rightCount) == 0){
			result = result.replaceAll("[（）]", "");
		}else if (leftCount > rightCount && s.charAt(0) == '（') {
			for(int j = 0;j<leftCount-rightCount;j++){
				result = result.replaceFirst("（", "");
			}
		}
        return result;
	}
	
	
	/**
	 * 删除各种特殊字符
	 * @param s
	 * @return
	 */
	public static String delSpecialChar(String s){
		String regEx="[*`~@#$%^*+=|{}\"':'\\[\\]<>?~·！@#￥%……*_\\-——+|{}＜＞【】'；：”“’，？]|（）";
		Pattern pattern = Pattern.compile(regEx);
		return pattern.matcher(s).replaceAll("");
	}

	/**
	 * 记录到unitInfo对象中
	 * @param unitInfo
	 * @param newUnitInfos
	 */
	public static void writeInfos(UnitInfo unitInfo, String[] newUnitInfos){
		unitInfo.setUnit(newUnitInfos[UnitStructure.unit]);
		unitInfo.setProvinceCode(newUnitInfos[UnitStructure.areaCode]);
		unitInfo.setTypeCode(newUnitInfos[UnitStructure.typeCode]);
		unitInfo.setUnitCode(newUnitInfos[UnitStructure.unitCode]);
		unitInfo.setRankCode(newUnitInfos[UnitStructure.rankCode]);
	}
	
	/**
	 * 单位和标准单位表中的单位比较
	 * @param s
	 * @param cp
	 * @return
	 */
	private String[] compareStr(String s, Comparator<String> cp){
		for(String[] eachArr:unitList){
			if(cp.compare(eachArr[UnitStructure.unit], s) == 1){
				return eachArr;
			}
		}
		return null;
	}
	
}
