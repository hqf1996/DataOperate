package com.updateTableUnit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.structure.PatentStructure;

public class PatentUpdateLegal {
	public static class PatentUpdateLegalMap extends Mapper<Object, Text, Text, Text>{
		Text outKety = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
			String[] infos = value.toString().split("\t");
			if(filePath.contains("legal")){
				if(infos.length >= 4){
					outKety.set(infos[0]);
					outValue.set(infos[infos.length-2]);
					context.write(outKety, outValue);
				}
			}else {
				outKety.set(infos[PatentStructure.application_no]);
				context.write(outKety, value);
			}
		}
	}
	
	
	public static class PatentUpdateLegalReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String legal = null;
			List<String[]> list = new ArrayList<String[]>();
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 1){
					legal = infos[0];
				}
				if(infos.length >= PatentStructure.totalNum){
					list.add(infos);
				}
			}
			if(!list.isEmpty()){
				if(legal != null){
					for(String[] infos:list){
						infos[PatentStructure.legal_status] = legal;
						outKey.set(StringUtils.join(infos, "\t"));
						context.write(outKey, NullWritable.get());
					}
				}else {
					for(String[] infos:list){
						outKey.set(StringUtils.join(infos, "\t"));
						context.write(outKey, NullWritable.get());
					}
				}
			}
		}
	}
	
}
