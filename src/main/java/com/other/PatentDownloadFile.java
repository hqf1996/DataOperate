package com.other;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.PatentStructure;

public class PatentDownloadFile {
	public static class  PatentDownloadFileMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			outKey.set(infos[PatentStructure.PATENT_ID]);
			outValue.set(infos[PatentStructure.PATENT_ID] + "\t" + infos[PatentStructure.publication_no]);
			context.write(outKey, outValue);
		}
	}
	
	public static class PatentDownloadFileReduce extends Reducer<Text, Text, Text, NullWritable>{
		int num = 1;
		Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value:values){
				outKey.set(num + "\t" + value.toString());
				context.write(outKey, NullWritable.get());
				num ++ ;
			}
		}
	}
}
