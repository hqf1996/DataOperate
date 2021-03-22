package com.zhanglb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;
import com.util.Util;

/**
 * 统计专家的成果数量
 * step3
 * @author yhj
 *
 */
public class CountExpertAchievement {
	
	/**
	 * 论文
	 * 输入数据来自 paper_all_expert
	 * @author yhj
	 */
	public static class CountExpertPaperMap extends Mapper<Object, Text, Text, Text>{
		public Text outValue = new Text();
		public Text outKey = new Text();
		public static final int STRNUM = PaperStructure.fourth_author_f + 1;
		Map<String, String> universityNameMap;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			universityNameMap = Util.getMapFromDir("university_change_name", "'", 0, 1);
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == STRNUM){
				String subjectCode = infos[PaperStructure.subject_code_f];
				String PaperId = infos[PaperStructure.PAPER_ID];
				for(int i = 0;i<4;i++){
					String expertId = infos[PaperStructure.first_author_f + i];
					String expertName = infos[PaperStructure.first_author + 4*i];
					String baidu = infos[PaperStructure.first_author_baidu + 4*i];
					String expertUnit = infos[PaperStructure.first_author_unit + 4*i];
					String typeCode = infos[PaperStructure.first_author_rank_code + i].substring(0, 2);
					if(!expertId.equals("null") && !expertName.equals("null") 
							&& expertName.indexOf("编辑部")== -1 && expertName.indexOf("不公开姓名")== -1 && expertName.indexOf("发明人")== -1 
							&& expertName.indexOf("项目组")== -1 && expertName.indexOf("课题组")== -1){
						if(i == 0 && expertUnit.equals("null")){
							expertUnit = infos[PaperStructure.unit];
							typeCode = infos[PaperStructure.type_code_f];
						}else if (expertUnit.equals("null")) {
							continue;
						}
						if(!expertUnit.equals("null")){
							//高校更名
							if(universityNameMap.containsKey(expertUnit)){
								expertUnit = universityNameMap.get(expertUnit);
								typeCode = "01";
							}
							if(baidu.equals("null")){
								outValue.set(expertId+ "\t" + expertUnit + "\t" + subjectCode + "\t99\t" + PaperId + "\t" + expertName + "\t" + typeCode);
							}else {
								outValue.set(expertId+ "\t" + expertUnit + "\t" + subjectCode + "\t" + baidu + "\t" + PaperId + "\t" + expertName + "\t" + typeCode); // id unit sunjectCode baidu
							}
							outKey.set(expertId);
							context.write(outKey,outValue);
						}
					}
				}
			}
		}
	}
	
	/**
	 * 专利
	 * 输入数据来自 patent_all_expert
	 * @author yhj
	 */
	public static class CountExpertPatentMap extends Mapper<Object, Text, Text, Text>{
		public Text outValue = new Text();
		public Text outKey = new Text();
		public static final int STRNUM = PatentStructure.fourth_author_f + 1;
		Map<String, String> universityNameMap;
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			universityNameMap = Util.getMapFromDir("university_change_name", "'", 0, 1);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == STRNUM){
				String subjectCode = infos[PatentStructure.subject_code_f];
				String PatentId = infos[PatentStructure.PATENT_ID];
				for(int i = 0;i<4;i++){
					String expertId = infos[PatentStructure.first_author_f + i];
					String expertName = infos[PatentStructure.inventor + 2*i];
					String baidu = "99";
					String expertUnit = infos[PatentStructure.inventor_unit + 2*i]; //直接将专利的单位作为专家的单位
					String typeCode = infos[PatentStructure.first_author_rank_code].substring(0, 2); //直接将专利的单位类型作为专家的单位类型
					if(!expertId.equals("null") && !expertName.equals("null") 
							&& expertName.indexOf("编辑部")== -1 && expertName.indexOf("不公开姓名")== -1 && expertName.indexOf("发明人")== -1 
							&& expertName.indexOf("项目组")== -1 && expertName.indexOf("课题组")== -1){
						if(i == 0 && expertUnit.equals("null")){
							expertUnit = infos[PatentStructure.applicant];
							typeCode = infos[PatentStructure.type_code_f];
						}else if (expertUnit.equals("null")) {
							continue;
						}
						if(!expertUnit.equals("null")){
							if(universityNameMap.containsKey(expertUnit)){
								expertUnit = universityNameMap.get(expertUnit);
								typeCode = "01";
							}
							outValue.set(expertId+ "\t" + expertUnit + "\t" + subjectCode + "\t" + baidu + "\t" + PatentId + "\t" + expertName + "\t" + typeCode);
							outKey.set(expertId);
							context.write(outKey,outValue);
						}
					}
				}
			}
		}
	}
	
	/**
	 * 项目
	 * 输入数据来自 project_all_expert
	 * @author yhj
	 */
	public static class CountExpertProjectMap extends Mapper<Object, Text, Text, Text>{
		public Text outValue = new Text();
		public Text outKey = new Text();
		public static final int STRNUM = ProjectStructure.fourth_author_f + 1;
		Map<String, String> universityNameMap;
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			universityNameMap = Util.getMapFromDir("university_change_name", "'", 0, 1);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == STRNUM){
				String subjectCode = infos[ProjectStructure.subject_code_f];
				String ProjectId = infos[ProjectStructure.PROJECT_ID];
				for(int i = 0;i<4;i++){
					String expertId = infos[ProjectStructure.first_author_f + i];
					String expertName = infos[ProjectStructure.leader + 2*i];
					String baidu = "99";
					String expertUnit = infos[ProjectStructure.leader_unit + 2*i];
					String typeCode = infos[ProjectStructure.first_author_rank_code + i].substring(0, 2);
					if(!expertId.equals("null") && !expertName.equals("null") 
							&& expertName.indexOf("编辑部")== -1 && expertName.indexOf("不公开姓名")== -1 && expertName.indexOf("发明人")== -1 
							&& expertName.indexOf("项目组")== -1 && expertName.indexOf("课题组")== -1){
						if(i == 0 && expertUnit.equals("null")){
							expertUnit = infos[ProjectStructure.unit];
							typeCode = infos[ProjectStructure.type_code_f];
						}else if (expertUnit.equals("null")) {
							continue;
						}
						if(!expertUnit.equals("null")){
							if(universityNameMap.containsKey(expertUnit)){
								expertUnit = universityNameMap.get(expertUnit);
								typeCode = "01";
							}
							outValue.set(expertId+ "\t" + expertUnit + "\t" + subjectCode + "\t" + baidu + "\t" + ProjectId + "\t" + expertName + "\t" + typeCode);
							outKey.set(expertId);
							context.write(outKey,outValue);
						}
					}
				}
			}
		}
	}
	
	
	public static class CountExpertPaperReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Map<String, Integer> subjectIndexMap = new HashMap<String, Integer>();
		public Text outText = new Text();
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			subjectIndexMap.put("A1", 0);
			subjectIndexMap.put("A2", 1);
			subjectIndexMap.put("A3", 2);
			subjectIndexMap.put("A4", 3);
			subjectIndexMap.put("A5", 4);
			subjectIndexMap.put("A9", 5);
			subjectIndexMap.put("C1", 6);
			subjectIndexMap.put("C2", 7);
			subjectIndexMap.put("Z1", 8);
			subjectIndexMap.put("Z9", 9);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int[] subjectCount = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
			int paperCount = 0;
			String[] infos = null;
			for(Text value:values){
				infos = value.toString().split("\t");
				String[] subjects = infos[2].split(";");
				for(int i=0;i<subjects.length;i++){
					if(subjectIndexMap.containsKey(subjects[i])){
						subjectCount[subjectIndexMap.get(subjects[i])]++;
					}
				}
				paperCount++;
			}
			if(infos != null){
				StringBuffer subjectCountStr = new StringBuffer();
				for(int i = 0;i<subjectCount.length;i++){
					subjectCountStr.append(subjectCount[i]);
					subjectCountStr.append("\t");
				}
				outText.set(infos[0] + "\t" + infos[1] + "\t" + paperCount + "\t0\t0\t" + subjectCountStr.toString() + infos[3] + "\t" + infos[5] + "\t" + infos[6]);
				context.write(outText, NullWritable.get());
			}
		}
	}
	
	
	public static class CountExpertPatentReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Map<String, Integer> subjectIndexMap = new HashMap<String, Integer>();
		public Text outText = new Text();
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			subjectIndexMap.put("A1", 0);
			subjectIndexMap.put("A2", 1);
			subjectIndexMap.put("A3", 2);
			subjectIndexMap.put("A4", 3);
			subjectIndexMap.put("A5", 4);
			subjectIndexMap.put("A9", 5);
			subjectIndexMap.put("C1", 6);
			subjectIndexMap.put("C2", 7);
			subjectIndexMap.put("Z1", 8);
			subjectIndexMap.put("Z9", 9);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int[] subjectCount = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
			int patentCount = 0;
			String[] infos = null;
			for(Text value:values){
				infos = value.toString().split("\t");
				String[] subjects = infos[2].split(";");
				for(int i=0;i<subjects.length;i++){
					if(subjectIndexMap.containsKey(subjects[i])){
						subjectCount[subjectIndexMap.get(subjects[i])]++;
					}
				}
				patentCount++;
			}
			if(infos != null){
				StringBuffer subjectCountStr = new StringBuffer();
				for(int i = 0;i<subjectCount.length;i++){
					subjectCountStr.append(subjectCount[i]);
					subjectCountStr.append("\t");
				}
				outText.set(infos[0] + "\t" + infos[1] + "\t0\t" + patentCount +  "\t0\t" + subjectCountStr.toString() + infos[3] + "\t" + infos[5] + "\t" + infos[6]);
				context.write(outText, NullWritable.get());
			}
		}
	}
	
	
	public static class CountExpertProjectReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Map<String, Integer> subjectIndexMap = new HashMap<String, Integer>();
		public Text outText = new Text();
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			subjectIndexMap.put("A1", 0);
			subjectIndexMap.put("A2", 1);
			subjectIndexMap.put("A3", 2);
			subjectIndexMap.put("A4", 3);
			subjectIndexMap.put("A5", 4);
			subjectIndexMap.put("A9", 5);
			subjectIndexMap.put("C1", 6);
			subjectIndexMap.put("C2", 7);
			subjectIndexMap.put("Z1", 8);
			subjectIndexMap.put("Z9", 9);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int[] subjectCount = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
			int projectCount = 0;
			String[] infos = null;
			for(Text value:values){
				infos = value.toString().split("\t");
				String[] subjects = infos[2].split(";");
				for(int i=0;i<subjects.length;i++){
					if(subjectIndexMap.containsKey(subjects[i])){
						subjectCount[subjectIndexMap.get(subjects[i])]++;
					}
				}
				projectCount++;
			}
			if(infos != null){
				StringBuffer subjectCountStr = new StringBuffer();
				for(int i = 0;i<subjectCount.length;i++){
					subjectCountStr.append(subjectCount[i]);
					subjectCountStr.append("\t");
				}
				outText.set(infos[0] + "\t" + infos[1] + "\t0\t0\t" + projectCount +  "\t" + subjectCountStr.toString() + infos[3] + "\t" + infos[5] + "\t" + infos[6]);
				context.write(outText, NullWritable.get());
			}
		}
	}
}
