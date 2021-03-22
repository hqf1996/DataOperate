package com.other;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.PatentStructure;

public class ExpertPatentUnit {
	public static class ExpertPatentUnitMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String unit = infos[PatentStructure.applicant];
			if(unit.length() > 3){
				context.write(new Text(unit), NullWritable.get());
			}
		}
	}
	
	public static class ExpertPatentUnitReduce extends Reducer<Text, NullWritable, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new Text("null\t" + key.toString()), NullWritable.get());
		}
	}
}
