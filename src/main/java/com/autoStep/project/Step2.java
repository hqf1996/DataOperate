package com.autoStep.project;

import com.structure.ExpertStructure;
import com.structure.ProjectStructure;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * 
 * step2:给四个作者根据专家表加上单位
 * 输入: expert表  step的输出
 * 输出:
 * @author yhj
 */
public class Step2 {
	public static class Step2Map extends Mapper<Object, Text, Text, Text>{
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
				authorNameIndex = ProjectStructure.leader + authorNum*2;
			}
		}
		// 输入的是expert表和上一步的输出文件
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			// 根据数组长度判断读取的是哪一个文件（expert或者上一步的输出文件）
			// 上一步的输出文件
			// key为专家名+单位名
			// value为输入的信息
			if(infos.length == ProjectStructure.fourth_author_f-3){
				infos[authorNameIndex] = infos[authorNameIndex].replaceAll("\\d$", "").replaceAll("^\\)", "");
				keyOut.set(infos[authorNameIndex] + "\t" + infos[ProjectStructure.unit]);
				valueOut.set(StringUtils.join(infos, "\t"));
				context.write(keyOut, value);
			}
			// expert文件
			// key为专家名+单位名，value为专家单位
			if(infos.length == ExpertStructure.totalNum){
				keyOut.set(infos[ExpertStructure.name] + "\t" + infos[ExpertStructure.unit]);
				valueOut.set(infos[ExpertStructure.unit]);
				context.write(keyOut, valueOut);
			}
			// 上述两个判断均将key设置为专家名+单位名
			// 只是value不相同，
		}
	}
	
	public static class Step2Reduce extends Reducer<Text, Text, Text, NullWritable>{
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
					// 读取的是上一步产生的信息
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
					// 读取的是expert表的信息，表明在expert表中存在该专家
					if(infos.length == 1){
						unit = infos[0];
					}

					if(infos.length == ProjectStructure.fourth_author_f-3){
						resultList.add(infos);
					}
				}
				if(!resultList.isEmpty()){
					if(unit != null){//专家表存在该专家
						for(String[] infos:resultList){
							if(infos[authorUnitIndex].equals("null")){//该专家的单位是空的
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
