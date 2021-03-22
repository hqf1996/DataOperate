package com.autoStep.paper;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.PaperStructure;

/**
 * 
 * @author yhj
 * 根据论文的uuid补上专家id
 * hdfs://10.1.13.111:8020/user/mysqlTmp/step1out
 * hdfs://10.1.13.111:8020/user/mysqlTmp/step3out
 * hdfs://10.1.13.111:8020/user/mysqlTmp/step4out
 */
public class Step4 {
	public static class PaperExpertJoinMap extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 7){
				context.write(new Text(infos[3]), value);
			}
			if(infos.length ==  PaperStructure.fourth_author_f-3){
				context.write(new Text(infos[PaperStructure.PAPER_ID]), value);
			}
		}
	}
	
	public static class PaperExpertJoinReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] paperInfos = null;
			String[] expertIds = {"null", "null", "null", "null"};
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 7){
					int expertRole = Integer.parseInt(infos[5]);
					expertIds[expertRole-1] = infos[0]; 
				}
				if(infos.length ==  PaperStructure.fourth_author_f-3){
					paperInfos = infos;
				}
			}
			if(paperInfos != null){
				String result = StringUtils.join(paperInfos, "\t") + "\t" + StringUtils.join(expertIds, "\t");
				System.out.println(result);
				context.write(new Text(result), NullWritable.get());
			}
		}
	}
}
