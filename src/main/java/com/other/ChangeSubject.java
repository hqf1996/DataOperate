package com.other;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.MapTable;
import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.util.Util;

public class ChangeSubject {
	public static class ChangePaperSubjectMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		Map<String, String> subjectinfos; 
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			subjectinfos = Util.getMapFromDir("subject_paper_map", "'", 0, 1);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(subjectinfos.containsKey(infos[PaperStructure.zw_subject_code])){
				infos[PaperStructure.subject_code] = subjectinfos.get(infos[PaperStructure.zw_subject_code]);
			}else {
				infos[PaperStructure.subject_code] = "其他";
			}
			infos[PaperStructure.subject_code_f] = MapTable.subjectCodeMap.get(infos[PaperStructure.subject_code]);
			outKey.set(infos[PaperStructure.PAPER_ID]);
			outValue.set(StringUtils.join(infos, "\t"));
			context.write(outKey, outValue);
		}
	}
	
	public static class ChangePatentSubjectMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		Map<String, String> subjectinfos; 
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			subjectinfos = Util.getMapFromDir("subject_patent_map", "'", 0, 1);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String mainClassfication = infos[PatentStructure.main_classification_no];
			if(subjectinfos.containsKey(mainClassfication.substring(0, 4))){
				infos[PatentStructure.subject_code] = subjectinfos.get(mainClassfication.substring(0, 4));
			}else if (subjectinfos.containsKey(mainClassfication.substring(0, 3))) {
				infos[PatentStructure.subject_code] = subjectinfos.get(mainClassfication.substring(0, 3));
			}else {
				infos[PatentStructure.subject_code] = "其他";
			}
			String[] subjects = infos[PatentStructure.subject_code].split(";");
			infos[PatentStructure.subject_code_f] = MapTable.subjectCodeMap.get(subjects[0]);
			for(int i = 1;i<subjects.length;i++){
				infos[PatentStructure.subject_code_f] = infos[PatentStructure.subject_code_f] +  ";" + MapTable.subjectCodeMap.get(subjects[i]); 
			}
			outKey.set(infos[PatentStructure.PATENT_ID]);
			outValue.set(StringUtils.join(infos, "\t"));
			context.write(outKey, outValue);
		}
	}
	
	public static class ChangeSubjectReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value:values){
				context.write(value, NullWritable.get());
			}
		}
	}
	
}
