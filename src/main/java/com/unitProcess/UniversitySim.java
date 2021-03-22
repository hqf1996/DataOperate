package com.unitProcess;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;

public class UniversitySim extends UnitSimAbstract{
	
	public UniversitySim(List<String[]> unitList, List<String[]> cityList) {
		// TODO Auto-generated constructor stub
		this.unitList = unitList;
		this.cityList = cityList;
	}
	
	@SuppressWarnings("unchecked")
	public static List<String[]> topSimilarity(String unit, List<String[]> unitList, List<String[]> cityList, int topCount,SimilarityFun fun1, SimilarityFun fun2){
		String preUnit = unit;
		int p = -1;
		String areaCode = null;
		for(String[] infos: cityList){
			if(unit.indexOf(infos[UnitCityCodeMapStructure.unit]) > p){
				areaCode = infos[UnitCityCodeMapStructure.areaCode];
			}
		}
		Matcher matcher;
		Map<Double, HashSet<String>> map = new HashMap<Double, HashSet<String>>();
		for (int i = 0; i < 2; i++) {//循环两遍，一遍是整个字段算相似度，一个是取大学等字段前的字符串
			for (String[] infos : unitList) {
				if ((areaCode == null || infos[UnitStructure.areaCode].equals(areaCode))
						&& (infos[UnitStructure.typeCode].equals("01") || infos[UnitStructure.typeCode].equals("05")
								|| infos[UnitStructure.typeCode].equals("06"))) {
					double cos = fun1.run(unit, infos[UnitStructure.unit]);
					if(map.containsKey(cos)){
						map.get(cos).add(infos[UnitStructure.unit]);
					}else {
						HashSet<String> set = new HashSet<String>();
						set.add(infos[UnitStructure.unit]);
						map.put(cos, set);
					}
				}
			}
			if ((matcher = Pattern.compile(".*?(学校|学院|大学)").matcher(unit)).find()) {
				unit = matcher.group(0);
			} else {
				break;
			}
		}
		Heap<Map.Entry<Double, HashSet<String>>> heap = new Heap<>(new ArrayList<Map.Entry<Double, HashSet<String>>>(map.entrySet()), 
				new Comparator<Map.Entry<Double, HashSet<String>>>() {

			@Override
			public int compare(Entry<Double, HashSet<String>> o1, Entry<Double, HashSet<String>> o2) {
				// TODO Auto-generated method stub
				return o1.getKey() > o2.getKey() ? 1:-1;
			}
		});
		List<String[]> list = new ArrayList<String[]>();
		for(Object e:heap.getCount(topCount)){
			Map.Entry<Double, HashSet<String>> entry = (Map.Entry<Double, HashSet<String>>)e;
			Iterator<String> iterator = entry.getValue().iterator();
			while(iterator.hasNext()){
				String unitStand = iterator.next();
				double lcs = fun2.run(preUnit, unitStand)*1.0/unitStand.length();
				list.add(new String[]{unitStand, Double.toString(entry.getKey()), Double.toString(lcs)});
			}
		}
		return list;
 	}
	
	@Override
	public double run(String source, String target) {
		// TODO Auto-generated method stub
		return StringSimilarity.cosineSimilarity(source, target);
	}

	@Override
	boolean isRightTypeCode(String typeCode) {
		// TODO Auto-generated method stub
		return typeCode.equals("01") || typeCode.equals("05")|| typeCode.equals("06");
	}

	@Override
	String filterRegex() {
		// TODO Auto-generated method stub
		return ".*?(学校|学院|大学)";
	}
	
}
