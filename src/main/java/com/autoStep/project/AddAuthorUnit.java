package com.autoStep.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ExpertStructure;
import com.structure.ProjectStructure;

public class AddAuthorUnit {
	public static class AddAuthorUnitMap extends Mapper<Object, Text, Text, Text>{
		int authorNameIndex;
		Text keyOut = new Text();
		Text valueOut = new Text();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int authorNum = context.getConfiguration().getInt("author number", -1);
			//输出的不是一到四作
			if(authorNum == -1){
				throw new IllegalArgumentException("author number is erro with " + authorNum);
			}else {
				authorNameIndex = ProjectStructure.leader + authorNum*2;
			}
		} 
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == ProjectStructure.fourth_author_f-3){
				infos[authorNameIndex] = infos[authorNameIndex].replaceAll("\\d$", "").replaceAll("^\\)", "");
				keyOut.set(infos[authorNameIndex] + "\t" + infos[ProjectStructure.unit]);
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
	
	
	public static class AddAuthorUnitReduce extends Reducer<Text, Text, Text, NullWritable>{
		int authorNameIndex;
		int authorUnitIndex;
		int authorUnitRankCodeIndex;
		Text keyOut = new Text();
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int authorNum = context.getConfiguration().getInt("author number", -1);
			if(authorNum == -1){
				throw new IllegalArgumentException("author number is erro with " + authorNum);
			}else {
				authorNameIndex = ProjectStructure.leader + authorNum*2;
				authorUnitIndex = ProjectStructure.leader_unit + authorNum*2;
				authorUnitRankCodeIndex = ProjectStructure.first_author_rank_code + authorNum;
			}
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//防止空单位的量太多,导致task崩溃
			if(key.toString().split("\t")[1].equals("null")){
				for(Text value:values){
					String[] infos = value.toString().split("\t");
					if(infos.length == ProjectStructure.fourth_author_f-3){
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
					if(infos.length == ProjectStructure.fourth_author_f-3){
						resultList.add(infos);
					}
				}
				if(!resultList.isEmpty()){
					if(unit != null){
						for(String[] infos:resultList){
							if(infos[authorUnitIndex].equals("null")){
								infos[authorUnitIndex] = unit;
								infos[authorUnitRankCodeIndex] = infos[ProjectStructure.rank_code_f];
								System.out.println(infos[ProjectStructure.PROJECT_ID] + "\t" + infos[authorNameIndex] + "\t" + infos[authorUnitIndex] + "\t" + infos[authorUnitRankCodeIndex]);
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
