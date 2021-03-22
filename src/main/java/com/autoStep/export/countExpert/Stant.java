package com.autoStep.export.countExpert;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Stant {

	public static class StantMap extends Mapper<Object, Text, Text, NullWritable>{
		public Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			outKey.set("null\t"+value.toString());
			context.write(outKey, NullWritable.get());
		}
	}
}
