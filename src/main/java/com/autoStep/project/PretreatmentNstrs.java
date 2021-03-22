package com.autoStep.project;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ProjectStructure;
import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.structure.UniversityShortCallStructure;
import com.unitProcess.UnitInfo;
import com.unitProcess.UnitProcess;
import com.util.Util;

/**
 * 爬虫的数据预处理，关联分类信息，整理成数据库的格式(暂时不用的数据全部放在字段后面，以后可能会用)
 * 分布式缓存数据：mysqlOut(unit_areacode_map_new,unit_citycode_map,unit_provincecode_map,china_universities,project_type_map),projectAdd(projectArea.txt,projectJihua.txt,projectJihua2.txt)//来源提供
 * 数据输入:projectDetail
 * 数据输出:PretreatmentNstrs
 * @author yhj
 *
 */
public class PretreatmentNstrs {
	
	public static class Step1Map extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outvalue = new Text();
		List<String[]> cityinfos;
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
		Map<String, String> projectAreaMap = null;  //项目地区对应
		Map<String, String> projectJihua1Map = null;  //项目与国家项目类型对应
		Map<String, String> projectJihua2Map = null;  //项目与地方项目类型对应
		Map<String, String> projectTypeMap = null;   //项目目录与类型代码对应
		/**
		 * map之前的初始化，主要用于加载
		 */
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			cityinfos =  Util.getListFromDir("unit_citycode_map", "'",3);
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
			projectAreaMap = Util.getMapFromDir("projectArea.txt", "\t", 0, 1);
			projectJihua1Map = Util.getMapFromDir("projectJihua.txt", "\t", 0, 1);
			projectTypeMap = Util.getMapFromDir("project_type_map", "\t", 1, 0);   
			List<String[]> projectJihua2List = Util.getListFromDir("projectJihua2.txt", "\t", 3);
			projectJihua2Map = new HashMap<String, String>();
			//地方的
			for(String[] infos: projectJihua2List){
				if(infos[1].startsWith(infos[2])){
					projectJihua2Map.put(infos[0], infos[1]);
				}else {
					projectJihua2Map.put(infos[0], infos[2] + infos[1]);
				}
			}
			//将省份中文转为省份代码
			for(String key:projectAreaMap.keySet()){
				for(String[] provinceInfo: cityinfos){
					if(projectAreaMap.get(key).startsWith(provinceInfo[1])){
						projectAreaMap.put(key, provinceInfo[2]);
						break;
					}
				}
			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length != 12){
				return;
			}
			String[] result = new String[ProjectStructure.totalNum];
			for(int i = 0;i<result.length;i++){
				result[i] = "null";
			}
			result[ProjectStructure.name] = infos[0];
			result[ProjectStructure.member] = infos[6].replaceAll("\\(.*?\\)", "");
			result[ProjectStructure.type] = getTypeStr(infos[11], infos[0]);
			result[ProjectStructure.keywords_ch] = infos[9];
			result[ProjectStructure.keywords_en] = infos[10];
			result[ProjectStructure.abstract_ch] = infos[7];
			result[ProjectStructure.abstract_en] = infos[8];
			result[ProjectStructure.area_code] = getAreaCode(infos[11]);
			result[ProjectStructure.year] =infos[1];
			result[ProjectStructure.url] =infos[11];
			result[ProjectStructure.source] = "6";
			result[ProjectStructure.project_type_more] = getTypeCode(result[ProjectStructure.type]);
			result[ProjectStructure.project_type] = result[ProjectStructure.project_type_more].substring(0, 1);
			String[] authors = infos[6].split(";");
			for(int i = 0;i<authors.length && i<4;i++){
				if(authors[i].length() >= 100){ //单位不可能过长,否则这条记录有问题,前期来源爬取的时候有问题，后期解决可以去掉
					return;
				}
				Pattern pattern = Pattern.compile("(.*?)\\((.*?)\\)");
				Matcher matcher = pattern.matcher(authors[i]);
				if(matcher.find() && matcher.groupCount()>=2){ //该作者的单位存在
					result[ProjectStructure.leader + 2*i] = matcher.group(1).trim();
					UnitInfo unitInfo = unitProcess.dispose(matcher.group(2).trim());
					result[ProjectStructure.leader_unit + 2*i] = unitInfo.getUnit();
					result[ProjectStructure.first_author_rank_code + i] = unitInfo.getRankCode();
					if(i == 0){//将第一作者的单位作为项目的单位
						result[ProjectStructure.unit] = unitInfo.getUnit();
						result[ProjectStructure.unit_type] = unitInfo.getTypeCode() + "\tZ9\t" + unitInfo.join("\t") ;
					}
				}else { //单位不存在
					result[ProjectStructure.leader + 2*i] = authors[i].trim();
					result[ProjectStructure.first_author_rank_code + i] = "999";
					if(i == 0){
						result[ProjectStructure.unit_type] = "99" + "\tZ9\tnull\t99\t99\t99999999\t999\tnull";
					}
				}
			}
//			result = changeError(result);
			outKey.set(result[ProjectStructure.name] + result[ProjectStructure.leader]);
			outvalue.set(StringUtils.join(result, "\t"));
			System.out.println(outvalue.toString());
			context.write(outKey, outvalue);
		}
		
		@SuppressWarnings("unused")
		private String[] changeError(String[] result){
			if(isChinese(result[ProjectStructure.abstract_en])){
				if(result[ProjectStructure.abstract_en].indexOf(";") != -1 || result[ProjectStructure.abstract_en].indexOf("；") != -1){
					result[ProjectStructure.keywords_en] = result[ProjectStructure.keywords_ch];
					result[ProjectStructure.keywords_ch] = result[ProjectStructure.abstract_en];
					result[ProjectStructure.abstract_en] = "null";
					if(result[ProjectStructure.keywords_en].indexOf(";") == -1 && result[ProjectStructure.keywords_en].indexOf("；") == -1){
						result[ProjectStructure.keywords_en] = "null";
					}
				}
			}
			return result;
		}
		
		/**
		 * 判断是否含有中文
		 * @param str
		 * @return
		 */
		private boolean isChinese(String str){
			if (str == null) {
	            return false;
	        }
	        Pattern pattern = Pattern.compile("[\\u4E00-\\u9FBF]+");
	        return pattern.matcher(str.trim()).find();
		}
		
		/**
		 * 根据url对应projectArea,得到area_code
		 * @param url
		 * @return
		 */
		private String getAreaCode(String url){
			if(projectAreaMap.containsKey(url)){
				return projectAreaMap.get(url);
			}else {
				return "null";
			}
		}
		
		/**
		 * 根据项目类型得到其类型代码
		 * @param key
		 * @return
		 */
		private String getTypeCode(String typeStr) {
			if(projectTypeMap.containsKey(typeStr)){
				return projectTypeMap.get(typeStr);
			}else {
				return "699";
			}
		}
		
		/**
		 * 根据url和项目名字关联项目类型
		 * @param url
		 * @param name
		 * @return
		 */
		private String getTypeStr (String url, String name) {
			if(projectJihua1Map.containsKey(url)){
				return projectJihua1Map.get(url);
			}else {
				if(projectJihua2Map.containsKey(name)){
					return projectJihua2Map.get(name);
				}else{
					return "null";
				}
			}
		}
	}
	
	public static class Step1Reduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value: values){
				context.write(value, NullWritable.get());
				break;
			}
		}
	}
	
}
