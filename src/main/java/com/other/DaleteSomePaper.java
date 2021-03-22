package com.other;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.PaperStructure;

public class DaleteSomePaper {
	public static class DaleteSomePaperMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String name = infos[PaperStructure.name].replaceAll("（.*?）", "");
			if(!(name.endsWith("通知") || name.endsWith("召开") || name.contains("投稿") || name.contains("作者指南"))){
				outKey.set(infos[PaperStructure.PAPER_ID]);
				context.write(outKey, value);
			}
		} 
	}
	
	public static class DaleteSomePaperReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value:values){
				context.write(value, NullWritable.get());
			}
		}
	}
}
