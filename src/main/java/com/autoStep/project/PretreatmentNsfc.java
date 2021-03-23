package com.autoStep.project;

import com.structure.ProjectStructure;
import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.structure.UniversityShortCallStructure;
import com.unitProcess.UnitInfo;
import com.unitProcess.UnitProcess;
import com.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * 杰青优青的项目数据预处理
 * 分布式缓存数据：mysqlOut(unit_areacode_map_new,unit_citycode_map,unit_provincecode_map,china_universities,project_type_map)
 * 数据输入:dataJQYQ(通学提供)
 * 数据输出:PretreatmentJQYQ
 * @author yhj
 *
 */
public class PretreatmentNsfc {
	
	public static class Step1JQYQMap extends Mapper<Object, Text, Text, Text>{
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
		Text outKey = new Text();
		Text outvalue = new Text();
		Map<String, String> projectTypeMap = null;   //项目目录与类型代码对应
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
			projectTypeMap = Util.getMapFromDir("project_type_map", "\t", 1, 0);
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
			String[] infos = value.toString().split("\t");
			if(filePath.indexOf("classify") != -1){//网页内容需要解析
				String[] htmlInfos = parse(infos[2], infos[1]);
				outKey.set(infos[0]+htmlInfos[0]);
				outvalue.set(StringUtils.join(htmlInfos, "\t"));
				context.write(outKey, outvalue);
			}
			if(filePath.indexOf("data") != -1 && infos.length > 7){
				String[] projectInfos = new String[ProjectStructure.totalNum];
				for(int i = 0; i< ProjectStructure.totalNum; i++){
					projectInfos[i] = "null";
				}
				projectInfos[ProjectStructure.name] = infos[1];
				projectInfos[ProjectStructure.source_unit] = infos[2];
				UnitInfo unitInfo = unitProcess.dispose(infos[2]);
				projectInfos[ProjectStructure.leader] = infos[3];
				projectInfos[ProjectStructure.type] = infos[4];
				projectInfos[ProjectStructure.total_fund] = infos[5];
				projectInfos[ProjectStructure.year] = infos[6];
				projectInfos[ProjectStructure.keywords_ch] = infos[7];
				projectInfos[ProjectStructure.project_type_more] = getTypeCode(infos[4]);
				projectInfos[ProjectStructure.project_type] = projectInfos[ProjectStructure.project_type_more].substring(0, 1);
				projectInfos[ProjectStructure.unit] = unitInfo.getUnit();
				projectInfos[ProjectStructure.member] = projectInfos[ProjectStructure.leader];
				projectInfos[ProjectStructure.leader_unit] = projectInfos[ProjectStructure.unit];
				projectInfos[ProjectStructure.area_code] = unitInfo.getProvinceCode();
				projectInfos[ProjectStructure.unit_type] = unitInfo.getTypeCode() + "\tZ9\t" + unitInfo.join("\t");
				projectInfos[ProjectStructure.source] = "6";
				outKey.set(projectInfos[ProjectStructure.name] + projectInfos[ProjectStructure.leader]);
				outvalue.set(StringUtils.join(projectInfos ,"\t"));
				System.out.println(outvalue.toString());
				context.write(outKey, outvalue);
			}
		}
		
		/**
		 * 根据项目类型得到其类型代码
		 * @param typeStr
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
		 * 解析杰青优青网页
		 * @param html
		 * @return
		 */
		private String[] parse(String html, String url){
			Document document = Jsoup.parse(html);
			String[] result = {"null", "null", "null", "null", "null", url};
			getAbstractKeywords(document, result);
			getLeader(document, result);
			return result;
		}
		
		/**
		 * 解析得到摘要，关键字
		 * @param document
		 * @param result
		 */
		private void getAbstractKeywords(Document document, String[] result){
			Elements elements = document.getElementsByClass("zyao");
			if(checkElements(elements)){
				for(Element element:elements){
					String abstractType = getInfo(element.getElementsByClass("left"));
					String abstractInfo = getInfo(element.getElementsByClass("jben"));
					if(abstractType != null && abstractInfo != null){
						if(abstractType.equals("中文摘要")){
							result[1] = abstractInfo;
						}
						if(abstractType.equals("英文摘要")){
							result[2] = abstractInfo;
						}
					}
					String keyWordsType = getInfo(element.getElementsByTag("b"));
					String keyWords = getInfo(element.getElementsByClass("xmu"));
					if(keyWordsType != null && keyWords != null){
//						System.out.println(keyWordsType + "\t" + keyWords);
						if(keyWordsType.equals("中文主题词")){
							result[3] = keyWords;
						}
						if(keyWordsType.equals("英文主题词")){
							result[4] = keyWords;
						}
					}
				}
			}
		}
		
		/**
		 * 解析得到项目负责人
		 * @param document
		 * @param result
		 */
		private void getLeader(Document document, String[] result){
			Elements elements = document.getElementsByClass("jben");
			if(checkElements(elements)){
				for(Element element:elements){
					String type = getInfo(element.getElementsByTag("b"));
					if(type != null && type.equals("项目负责人")){
						String name = getInfo(element.getElementsByTag("a"));
						if(name != null){
							result[0] = name;
						}
					}
				}
			}
		}
		
		/**
		 * 检查标签是否存在
		 * @param elements
		 * @return
		 */
		private boolean checkElements(Elements elements){
			if(elements != null && elements.size() > 0){
				return true;
			}
			return false;
		}
		
		/**
		 * 获得标签下属于自身的内容
		 * @param elements
		 * @return
		 */
		private String getInfo(Elements elements){
			return elements.size() > 0 ?elements.get(0).ownText():null;
		}
		
	}
	
	public static class Step1JQYQReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] result = null;
			String[] htmlInfos = null;
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length >= ProjectStructure.totalNum){
					result = infos;
				}else {
					htmlInfos = infos;
				}
			}
			if(result != null){
				if(htmlInfos != null){
					result[ProjectStructure.abstract_ch] = htmlInfos[1];
					result[ProjectStructure.abstract_en] = htmlInfos[2];
					result[ProjectStructure.keywords_en] = htmlInfos[4];
					result[ProjectStructure.url] = htmlInfos[5];
				}
				for(int i = 0;i<result.length;i++){ //去除字段前后的空格
					result[i] = result[i].trim();
				}
				if(!result[ProjectStructure.name].equals("")){ //检查项目名是否为空
					outKey.set(StringUtils.join(result, "\t") + "\t" + result[ProjectStructure.rank_code_f] + "\tnull\tnull\tnull");
					System.out.println(outKey.toString());
					context.write(outKey, NullWritable.get());
				}
			}
		}
	}
}
