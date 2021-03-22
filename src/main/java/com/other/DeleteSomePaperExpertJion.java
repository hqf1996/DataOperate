package com.other;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DeleteSomePaperExpertJion {
	public static class DeleteSomePaperExpertJionMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 2){
				outKey.set(infos[0]);
				outValue.set(infos[1]);
				context.write(outKey, outValue);
			}else {
				outKey.set(infos[4]);
				context.write(outKey, value);
			}
		}
	}
	
	public static class DeleteSomePaperExpertJionReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			boolean isDelete = false;
			String name = null;
			List<String[]> list = new ArrayList<String[]>();
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 1){
					isDelete = true;
					name = infos[0];
				}else {
					list.add(infos);
				}
			}
			if(!isDelete){
				for(String[] infos:list){
					outKey.set(StringUtils.join(infos, "\t"));
					context.write(outKey, NullWritable.get());
				}
			}else {
				if(!list.isEmpty()){
					System.out.println("delete " + key.toString() + "\t" + name);
				}
			}
		}
	}
}
