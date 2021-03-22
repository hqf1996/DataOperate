package com.autoStep.paper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.MapTable;
import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.structure.UniversityShortCallStructure;
import com.unitProcess.UnitInfo;
import com.unitProcess.UnitProcess;
import com.util.Util;

/**
 * 
 * @author yhj
 * step1 : clean unit and subjectcode
 * 步骤1: 处理论文和专家的单位，还有对应论文相应的subjectcode,产生论文的uuid,去重论文
 * hdfs://10.1.13.111:8020/user/addData/paper
 * hdfs://10.1.13.111:8020/user/mysqlTmp/step1out
 */
public class PretreatmentZW {
	
	
	public static class CleanMapper extends Mapper<Object, Text, Text, Text>{
		
		static final int STRNUM = 40; 
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
		Map<String, String> subjectinfos = null;
		Map<String, String> journalinfos = null;
		Map<String, String> journalQuailtyMap = new HashMap<String, String>();
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/*unit表 单位表
			1'龙岩学院'35'00000001'01'011
				2'齐齐哈尔高等师范专科学校'23'00000002'01'011
				3'齐齐哈尔理工职业学院'23'00000003'01'011
				4'齐齐哈尔工程学院'23'00000004'01'011
				*/
			/* unit_citycode_map 单位城市表，后面的11 12 表示城市的代码 在单位表中有体现
			* 1'北京'11
			2'天津'12
			3'河北'13
			4'山西'14
			5'内蒙古'15
			6'辽宁'21
			7'吉林'22
			8'黑龙江'23*/

			/*学校简称表university_short_call
			* 1'上海交通大学'31'00002897'01'011'上海交大
			2'上海大学'31'00002882'01'011'上大
			3'中南财经政法大学'42'00002795'01'011'中南财经
			4'中国人民大学'11'00002791'01'011'人大
			5'中国地质大学'11;42'00002693'01'011'中国地质
			6'中国科学技术大学'34'00002675'01'011'中国科技大学
			*/
			/*subject_paper_map  论文中的代码与类别的映射表
			* A001'其他
			A002'其他
			A003'其他
			A004'其他
			A005'其他
			A006'其他
			A007'其他
			A008'其他
			A009'资源与环境
			A010'资源与环境
			*/
			/*期刊信息表 journal_information  一个期刊中会有很多论文
			1       计算机集成制造系统      Computer Integrated Manufacturing Systems       计算机集成制造系统-CIMS
			核心期刊;SA;Pж(AJ);EI;CSCD    FIRST   月刊    91306   1674137 1.583   0.95    5774
			中国兵器工业集团第210研究所  JSJJ     信息科技        电子信息科学综合        I       I000
			http://navi.cnki.net/KNavi/JournalDetail?pcode=CJFD&pykm=JSJJ
			 */
			unitList = Util.getListFromDir("unit", "\t", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "\t", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "\t", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
			
			subjectinfos = Util.getMapFromDir("subject_paper_map", "\t", 0, 1);
			journalinfos = Util.getMapFromDir("journal_information", "\t", 1, 5);
			journalQuailtyMap.put("SCI", "1");
			journalQuailtyMap.put("FIRST", "2");
			journalQuailtyMap.put("EI", "3");
			journalQuailtyMap.put("HEXIN", "4");
			journalQuailtyMap.put("OTHERS", "5");
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == STRNUM && isNeedPaper(infos[0])){
				String[] rankArr = new String[]{"999", "999", "999", "999"};
				String firstOrganization = infos[3];
				//论文单位规范化
				UnitInfo unitInfo = unitProcess.dispose(firstOrganization);
				infos[4] = unitInfo.getUnit();
				infos[33] = unitInfo.getProvinceCode();
				
				//百度学术专家单位规范化
				for(int i = 0;i<4;i++){
					int index = 7 + i*4;
					UnitInfo expertUnitInfo = unitProcess.dispose(infos[index]);
					infos[index] = expertUnitInfo.getUnit();
					rankArr[i] = expertUnitInfo.getRankCode();
				}
				
				//期刊质量字段补充
				infos[36] = getJournalQuality(infos[34]);
				
				//补充subjectCode字段
				infos[30] = getSubject(infos[29]); //29为知网subject的位置（其实为期刊号），30为佐创一级分类
				
				String subjectCode = subjectToCode(infos[30]);
				String result = StringUtils.join(infos, "\t") + "\t" + unitInfo.getTypeCode() + "\t" + subjectCode + "\t" + unitInfo.join("\t") + "\t" + StringUtils.join(rankArr, "\t");
//				System.out.println(result);
				context.write(new Text(infos[0]+infos[1]),new Text(result));
 			}
		}
		
		/**
		 * 判断是否是需要的期刊
		 * 论文名以"通知"，"召开"结尾或者论文名包括"投稿"和"作者指南"的都舍弃
		 * @param name
		 * @return
		 */
		public boolean isNeedPaper(String name){
			return !(name.endsWith("通知") || name.endsWith("召开") || name.contains("投稿") || name.contains("作者指南"));
		}
		
		/**
		 * 根据期刊名字得到期刊质量
		 * @param journalName
		 * @return journalQuality
		 */
		public String getJournalQuality(String journalName){
			if(!journalName.equals("null")){
				if(journalinfos.containsKey(journalName)){
					String newJournalQuality = journalQuailtyMap.get(journalinfos.get(journalName));
					if (newJournalQuality != null){
						return newJournalQuality;
					}
				}
				return "6";
			}
			return "null";
		}
		
		/**
		 * 根据知网的subjectCode得到佐创一级分类
		 * @param zwSubjectCode
		 * @return
		 */
		public String getSubject(String zwSubjectCode){
			if(subjectinfos.containsKey(zwSubjectCode)){
				return subjectinfos.get(zwSubjectCode);
			}else {
				return "其他";
			}
		}
		
		/**
		 * 将subject的中文转为相应的code
		 * @param subject
		 * @return
		 */
		public String subjectToCode(String subject){
			if(MapTable.subjectCodeMap.containsKey(subject)){
				return MapTable.subjectCodeMap.get(subject);
			}
			return "Z9";
		}
	}
	
	public static class CleanReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value:values){
				String result = "null\t" + UUID.randomUUID().toString() + "\t" + value.toString();
				System.out.println(result);
				context.write(new Text(result), NullWritable.get());
				break;
			}
		}
	}
}
