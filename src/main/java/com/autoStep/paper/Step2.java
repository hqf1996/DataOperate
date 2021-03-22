package com.autoStep.paper;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PaperStructure;
import com.util.Util;

/**
 * 
 * @author yhj
 * 提取出所有的专家,单位
 * hdfs://10.1.13.111:8020/user/mysqlTmp/step1out
 * hdfs://10.1.13.111:8020/user/mysqlTmp/step2out
 */
public class Step2 {
	
	public static class ExportAuthorsMap extends Mapper<Object, Text, Text, NullWritable>{
		static final int STRNUM = PaperStructure.fourth_author_f-3;
		Map<String, String> universityNameMap;
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			universityNameMap = Util.getMapFromDir("university_change_name", "'", 0, 1);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if (infos.length == STRNUM){
				String paperId = infos[PaperStructure.PAPER_ID];
				String paperName = infos[PaperStructure.name];
				String unit = infos[PaperStructure.unit];
				String source = "41";
				for(int i = 0;i<4;i++){
					String expertName = infos[PaperStructure.first_author + i*4];
					String expertUnit = infos[PaperStructure.first_author + i*4 + 1];
					String expertRankCode = infos[PaperStructure.first_author_rank_code + i];  
					int expertRole = i+1;
					if(!expertName.equals("null")){
						if(i == 0){ //如果是第一作者
							if(expertUnit.equals("null")){ //如果一作的单位为空
								expertUnit = unit;
								expertRankCode = infos[PaperStructure.rank_code_f];
							}
							source = "4";
						}else { //如果是其他作者
							source = "41";
						}
						if(!expertUnit.equals("null") && (expertRankCode.charAt(2) == '1' ||expertRankCode.equals("022") || expertRankCode.equals("032"))){
							if(universityNameMap.containsKey(expertUnit)){
								System.out.print("高校变更名" + expertUnit);
								expertUnit = universityNameMap.get(expertUnit);
								System.out.print("\t" + expertUnit);
							}
							if(!(expertName.contains("课题组") || expertName.contains("编辑部") || expertName.contains("项目组") || expertName.contains("不公开姓名") || expertName.contains("发明人"))){
								expertName = expertName.replaceAll("^\\(.*?\\)|^(\\d)*\\.|^\\)|(\\d)*$", "");
								String result = expertName + "\t" + expertUnit + "\t" + paperId + "\t" + paperName + "\t" + expertRole + "\t" + source;
								System.out.println(result);
								context.write(new Text(result), NullWritable.get());
							}
						}
					}
				}
			}
		}
	}
}
