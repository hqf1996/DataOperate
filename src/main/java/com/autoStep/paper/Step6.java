package com.autoStep.paper;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author yhj
 * 提取增加的论文专家级关联记录
 * hdfs://10.1.13.111:8020/user/mysqlTmp/step3out
 * hdfs://10.1.13.111:8020/user/mysqlTmp/addExpertPaperJoin_localdate
 */
public class Step6 {
	
	public static class ExportAddPaperExpertJoinMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 7){
				if(infos[1].length() > 255){
					infos[1] = infos[1].substring(0, 255);
				}
				if(infos[4].length() > 255){
					infos[4] = infos[4].substring(0, 255);
				}
				String result = "null" + "\t" + infos[0] + "\t" + infos[1] + "\t" + infos[5] + "\t" + infos[3] + "\t" + infos[4];
				context.write(new Text(result.replace("'", "")), NullWritable.get());
			}
		}
		
	}
}
