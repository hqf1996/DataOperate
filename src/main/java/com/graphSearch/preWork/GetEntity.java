package com.graphSearch.preWork;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class GetEntity {
	public static class GetEntityMap extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			context.write(new Text(infos[1]), new Text(infos[3]));
			context.write(new Text(infos[2]), new Text(infos[5]));
		}
	}
	
	public static class GetEntityReduce extends Reducer<Text, Text, Text, NullWritable>{
		static int count = 0;
		Text keyOut = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String valueOut = "";
			for(Text value:values){
				valueOut = value.toString();
				break;
			}
			keyOut.set(count++ + "\t" + key.toString() + "\t" + valueOut);
			context.write(keyOut, NullWritable.get());
		} 
	}
}
