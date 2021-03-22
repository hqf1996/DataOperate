package com.autoStep.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinExpertPaperToGe {

	public static class JoinExpertPaperToGeMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		public Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 6){
				outKey.set(infos[4]);
				context.write(outKey, value);
			}
			if (infos.length == 42) {
				outKey.set(infos[1]);
				outValue.set(infos[8] + "\t" + infos[3] + "\t" + infos[36] + "\t" + infos[34]);
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class JoinExpertPaperToGeReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Text outKey = new Text();
		public Text outValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String paperInfo = null;
			List<String[]> list = new ArrayList<String[]>();
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 6){
					list.add(infos);
				}
				if(infos.length == 4){
					paperInfo = value.toString();
				}
			}
			if(!list.isEmpty() && paperInfo != null){
				for(String[] infos: list){
					outKey.set(infos[1] + "\t" + infos[2] + "\t" + infos[3] + "\t" + infos[4] + "\t" + infos[5] + "\t" + paperInfo);
					context.write(outKey, NullWritable.get());
				}
			}
		}
	}
}
