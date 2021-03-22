package com.autoStep.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ExpertStructure;

/**
 * step4 给提取出的专家补上uuid,若专家表中已经存在的，用专家表的uuid补，否则自己生成一个补上
 * 数据来源：step3和/user/mysqlOut/expert
 * 数据产生：step4
 * @author yhj
 *
 */
public class Step4 {
	
	public static class Step4Map extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 6){
				context.write(new Text(infos[0]+infos[1]), value);
			}
			if(infos.length == ExpertStructure.totalNum){
				context.write(new Text(infos[ExpertStructure.name]+infos[ExpertStructure.unit]), new Text(infos[ExpertStructure.EXPERT_ID]));
			}
		}
	}
	
	public static class Step4Reduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> list = new ArrayList<>();
			String expertUUid = null;
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 6){
					list.add(infos);
				}else {
					if(infos.length ==1 ){
						expertUUid = infos[0];
					}
				}
			}
			if(expertUUid == null){//专家表中不存在该专家
				String newUUId = UUID.randomUUID().toString();
				for(String[] infos:list){
					String result = newUUId + "\t" + StringUtils.join(infos, "\t");
					System.out.println("create a new uuid :" + result);
					context.write(new Text(result), NullWritable.get());
				}
			}else {//专家表中存在该专家
				for(String[] infos:list){ 
					String result = expertUUid + "\t" + StringUtils.join(infos, "\t");
					System.out.println("uuin from expert table :" + result);
					context.write(new Text(result), NullWritable.get());
				}
			}
		}
	}
}
