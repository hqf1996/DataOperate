package com.autoStep.paper;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author yhj
 * map寻找某个字段的值
 */
public class FindStep {
	public static class FindMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[1].equals("7fcd1f71-8608-4cb5-890a-acf7261c8ea7")){
				System.out.println(value.toString());
				context.write(value, NullWritable.get());
			}
		}
	}
}
