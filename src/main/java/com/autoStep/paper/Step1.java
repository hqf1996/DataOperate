package com.autoStep.paper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ExpertStructure;
import com.structure.PaperStructure;

/**
 * 
 * @author yhj
 * step1:给四个作者根据专家表加上单位
 * 输入: expert表  step的输出
 * 输出:
 */
public class Step1 {
	public static class AddAuthorUnitMap extends Mapper<Object, Text, Text, Text>{
		int authorNameIndex;
		Text keyOut = new Text();
		Text valueOut = new Text();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int authorNum = context.getConfiguration().getInt("author number", -1);
			//输出的不是一到四作，取值范围为0到3
			if(authorNum == -1 || authorNum < 0 || authorNum > 3){
				throw new IllegalArgumentException("author number is erro with " + authorNum);
			}else {
				authorNameIndex = PaperStructure.first_author + authorNum*4;
			}
		} 
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == PaperStructure.fourth_author_f-3){
				infos[authorNameIndex] = infos[authorNameIndex].replaceAll("\\d$", "").replaceAll("^\\)", "");
				keyOut.set(infos[authorNameIndex] + "\t" + infos[PaperStructure.unit]);
				valueOut.set(StringUtils.join(infos, "\t"));
				context.write(keyOut, value);
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
				authorNameIndex = PaperStructure.first_author + authorNum*4;
				authorUnitIndex = PaperStructure.first_author_unit + authorNum*4;
				authorUnitRankCodeIndex = PaperStructure.first_author_rank_code + authorNum;
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
					if(infos.length == PaperStructure.fourth_author_f-3){
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
					if(infos.length == PaperStructure.fourth_author_f-3){
						resultList.add(infos);
					}
				}
				if(!resultList.isEmpty()){
					if(unit != null){//专家表存在该专家
						for(String[] infos:resultList){
							if(infos[authorUnitIndex].equals("null")){
								infos[authorUnitIndex] = unit;
								infos[authorUnitRankCodeIndex] = infos[PaperStructure.rank_code_f];
								System.out.println(infos[PaperStructure.PAPER_ID] + "\t" + infos[authorNameIndex] + "\t" + infos[authorUnitIndex] + "\t" + infos[authorUnitRankCodeIndex]);
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
