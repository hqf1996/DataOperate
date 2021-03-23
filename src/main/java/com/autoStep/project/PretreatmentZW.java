package com.autoStep.project;

import com.structure.MapTable;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);

			subjectinfos = Util.getMapFromDir("subject_paper_map", "'", 0, 1);
			journalinfos = Util.getMapFromDir("journal_information", "\t", 1, 5);
			journalQuailtyMap.put("SCI", "1");
			journalQuailtyMap.put("FIRST", "2");
			journalQuailtyMap.put("EI", "3");
			journalQuailtyMap.put("HEXIN", "4");
			journalQuailtyMap.put("OTHERS", "5");
		}

		@Override
		protected void map(Object key, Text value, Context context)
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
				Context context) throws IOException, InterruptedException {
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
