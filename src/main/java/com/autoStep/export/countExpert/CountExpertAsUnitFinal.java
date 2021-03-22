package com.autoStep.export.countExpert;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author yhj
 * Step3 这一步不需要了
 */
public class CountExpertAsUnitFinal {

	public static class CountExpertAsUnitFinalMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		public Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			outKey.set(infos[1]);
			outValue.set(infos[0] + " " + infos[2] + " " + infos[3] + " " + infos[4] + " " + infos[5] + " " + infos[6] + " " + infos[7] + " " + infos[8]);
			context.write(outKey, outValue);
		}
	}
	
	public static class CountExpertAsUnitFinalReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			StringBuilder result = new StringBuilder(key.toString());
			for(Text value:values){
				result.append("\t");
				result.append(value.toString());
			}
			outKey.set(result.toString());
			context.write(outKey, NullWritable.get());
		}
	}
}
