package com.autoStep.export;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountProjectBaseUnit {
	
	public static class CountProjectBaseUnitMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		public Text outValue = new Text();
		static final int unitP = 8; //project 8 patent 27
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] info = value.toString().split("\t");
			outKey.set(info[unitP]);
			outValue.set(info[1]);
			context.write(outKey, outValue);
		}
	}
	
	public static class CountProjectBaseUnitReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String ids = key.toString();
			for(Text value:values){
				ids = ids + "\t" + value.toString();
			}
			outKey.set(ids);
			context.write(outKey, NullWritable.get());
		}
	}
}
