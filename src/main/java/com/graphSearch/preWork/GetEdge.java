package com.graphSearch.preWork;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class GetEdge {
	public static class GetEdgeMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			context.write(new Text(infos[4]), NullWritable.get());
		}
	}
	
	public static class GetEdgeReduce extends Reducer<Text, NullWritable, Text, NullWritable>{
		static int count = 0;
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text(count++ + "\t" + key.toString()), NullWritable.get());
		}
	}
}
