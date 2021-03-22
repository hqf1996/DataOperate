package com.updateTableUnit;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;
import com.util.Util;

/**
 * step3 提取出所有的专拣
 * step1 的所有输出(即所有的成果表)
 * @author yhj
 *
 */
public class ExportExpert {
	public static class ExportExpertMap extends Mapper<Object, Text, Text, NullWritable>{
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
			String filepath = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(filepath.contains("paper")){
				String paperId = infos[PaperStructure.PAPER_ID];
				String paperName = infos[PaperStructure.name];
				String unit = infos[PaperStructure.unit];
				String source = "41";
				String paperRankCode = infos[PaperStructure.rank_code_f];
				for(int i = 0;i<4;i++){
					String expertName = infos[PaperStructure.first_author + i*4];
					String expertUnit = infos[PaperStructure.first_author + i*4 + 1];
					String expertRankCode = infos[PaperStructure.first_author_rank_code + i];  
					int expertRole = i+1;
					if(!expertName.equals("null")){
						if(i == 0){ //如果是第一作者
							if(expertUnit.equals("null")){ //如果一作的单位为空
								expertUnit = unit;
								expertRankCode = paperRankCode;
							}
							source = "4";
						}else { //如果是其他作者
							source = "41";
						}
						if(!expertUnit.equals("null") && (expertRankCode.charAt(2) == '1' ||expertRankCode.equals("022") || expertRankCode.equals("032"))){
							if(universityNameMap.containsKey(expertUnit)){
								System.out.print("高校变更名\t" + expertUnit);
								expertUnit = universityNameMap.get(expertUnit);
								System.out.print("\t" + expertUnit);
							}
							String result = expertName + "\t" + expertUnit + "\t" + paperId + "\t" + paperName + "\t" + expertRole + "\t" + source;
							System.out.println(result);
							context.write(new Text(result), NullWritable.get());
						}
					}
				}	
			}
			
			if(filepath.contains("patent")){ //专利只提取第一作者
				String patentId = infos[PatentStructure.PATENT_ID];
				String patentName = infos[PatentStructure.name];
				String unit = infos[PatentStructure.applicant];
				String source = "31";
				String patentRankCode = infos[PatentStructure.rank_code_f];
				for(int i = 0;i<4;i++){
					String expertName = infos[PatentStructure.inventor + i*2];
					String expertUnit = infos[PatentStructure.inventor_unit + i*2];
					String expertRankCode = infos[PatentStructure.first_author_rank_code + i];
					int expertRole = i+1;
					if(!expertName.equals("null")){
						if(i == 0){ //如果是第一作者
							if(expertUnit.equals("null")){ //如果一作的单位为空
								expertUnit = unit;
								expertRankCode = patentRankCode;
							}
							source = "3";
						}else {
							source = "31";
						}
						if(!expertUnit.equals("null") && (expertRankCode.charAt(2) == '1' ||expertRankCode.equals("022") || expertRankCode.equals("032"))){
							if(universityNameMap.containsKey(expertUnit)){
								System.out.print("高校变更民" + expertUnit);
								expertUnit = universityNameMap.get(expertUnit);
								System.out.print("\t" + expertUnit);
							}
							String result = expertName + "\t" + expertUnit + "\t" + patentId + "\t" + patentName + "\t" + expertRole + "\t" + source;
							System.out.println(result);
							context.write(new Text(result), NullWritable.get());
						}
					}
				}
				
			}
			
			if(filepath.contains("project")){ //项目提取作者
				String projectId = infos[ProjectStructure.PROJECT_ID];
				String projectName = infos[ProjectStructure.name];
				String unit = infos[ProjectStructure.unit];
				String projectRankCode = infos[ProjectStructure.rank_code_f];
				String source = "21";
				for(int i=0;i<4;i++){
					String expertName = infos[ProjectStructure.leader + i*2];
					String expertUnit = infos[ProjectStructure.leader + i*2 + 1];
					String expertRankCode = infos[ProjectStructure.first_author_rank_code + i];  
					int expertRole = i+1;
					if(!expertName.equals("null")){
						if(i == 0){ //如果是第一作者
							if(expertUnit.equals("null")){ //如果一作的单位为空
								expertUnit = unit;
								expertRankCode = projectRankCode;
							}
							source = "2";
						}else { //如果是其他作者
							source = "21";
						}
						if(!expertUnit.equals("null") && (expertRankCode.charAt(2) == '1' ||expertRankCode.equals("022") || expertRankCode.equals("032"))){
							if(universityNameMap.containsKey(expertUnit)){
								System.out.print("高校变更名" + expertUnit);
								expertUnit = universityNameMap.get(expertUnit);
								System.out.print("\t" + expertUnit);
							}
							String result = expertName + "\t" + expertUnit + "\t" + projectId + "\t" + projectName + "\t" + expertRole + "\t" + source;
							System.out.println(result);
							context.write(new Text(result), NullWritable.get());
						}
					}
				}
			}
		}
	} 
}
