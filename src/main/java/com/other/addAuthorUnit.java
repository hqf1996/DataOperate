package com.other;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ExpertStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;

public class addAuthorUnit {
	public static class projectAddAuthorUnitMap extends Mapper<Object, Text, Text, Text>{
		Text keyOut = new Text();
		Text valueOut = new Text();
		int authorNameP = ProjectStructure.fourth_member;
		int authorUnitP = ProjectStructure.fourth_member_unit;
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == ProjectStructure.fourth_author_f+1){
				infos[authorNameP] = infos[authorNameP].replaceAll("\\d$", "").replaceAll("^\\)", "");
				keyOut.set(infos[authorNameP] + infos[ProjectStructure.unit]);
				valueOut.set(value);
				context.write(keyOut, valueOut);
			}
			if(infos.length == ExpertStructure.totalNum){
				keyOut.set(infos[ExpertStructure.name] + infos[ExpertStructure.unit]);
				valueOut.set(infos[ExpertStructure.unit]);
				context.write(keyOut, valueOut);
			}
		}
	}
	
	public static class projectAddAuthorUnitReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text keyOut = new Text();
		int authorNameP = ProjectStructure.fourth_member;
		int authorUnitP = ProjectStructure.fourth_member_unit;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String unit = null;
			List<String[]> resultList = new ArrayList<String[]>();
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 1){
					unit = infos[0];
				}
				if(infos.length == ProjectStructure.fourth_author_f+1){
					resultList.add(infos);
				}
			}
			if(!resultList.isEmpty()){
				if(unit != null){
					for(String[] infos:resultList){
						if(infos[authorUnitP].equals("null")){
							infos[authorUnitP] = unit;
							System.out.println(infos[ProjectStructure.PROJECT_ID] + "\t" + unit);
						}
						keyOut.set(StringUtils.join(Arrays.copyOf(infos, infos.length-8), "\t"));
						context.write(keyOut, NullWritable.get());
					}
				}else {
					for(String[] infos:resultList){
						keyOut.set(StringUtils.join(Arrays.copyOf(infos, infos.length-8), "\t"));
						context.write(keyOut, NullWritable.get());
					}
				}
			}
		}
	}
	
	public static class PatentAddAuthorUnitMap extends Mapper<Object, Text, Text, Text>{
		Text keyOut = new Text();
		Text valueOut = new Text();
		int authorNameP = PatentStructure.fourth_inventor;
		int authorUnitP = PatentStructure.fourth_inventor_unit;
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == PatentStructure.fourth_author_f-3){
				keyOut.set(infos[authorNameP] + "\t" + infos[PatentStructure.applicant]);
				valueOut.set(value);
				context.write(keyOut, valueOut);
			}
			if(infos.length == ExpertStructure.totalNum){
				keyOut.set(infos[ExpertStructure.name] + "\t" + infos[ExpertStructure.unit]);
				valueOut.set(infos[ExpertStructure.unit]);
				context.write(keyOut, valueOut);
			}
		}
	}

	public static class PatentAddAuthorUnitReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text keyOut = new Text();
		int authorNameP = PatentStructure.fourth_inventor;
		int authorUnitP = PatentStructure.fourth_inventor_unit;
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(key.toString().split("\t")[1].equals("null")){
				for(Text value:values){
					String[] infos = value.toString().split("\t");
					if(infos.length == PatentStructure.fourth_author_f-3){
						keyOut.set(StringUtils.join(infos, "\t"));
						context.write(keyOut, NullWritable.get());
					}
				}
			}else {
				String unit = null;
				List<String[]> resultList = new ArrayList<String[]>();
				for(Text value:values){
					String[] infos = value.toString().split("\t");
					if(infos.length == 1){
						unit = infos[0];
					}
					if(infos.length == PatentStructure.fourth_author_f-3){
						resultList.add(infos);
					}
				}
				if(!resultList.isEmpty()){
					if(unit != null){
						for(String[] infos:resultList){
							if(infos[authorUnitP].equals("null")){
								infos[authorUnitP] = unit;
								System.out.println(infos[PatentStructure.PATENT_ID] + "\t" + unit);
							}
							keyOut.set(StringUtils.join(infos, "\t"));
							context.write(keyOut, NullWritable.get());
						}
					}else {
						for(String[] infos:resultList){
							keyOut.set(StringUtils.join(infos, "\t"));
							context.write(keyOut, NullWritable.get());
						}
					}
				}
			}
		}
	}
}
