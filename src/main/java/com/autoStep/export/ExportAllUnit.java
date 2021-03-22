package com.autoStep.export;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;

public class ExportAllUnit {
	public static class ExportAllUnitMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		public Text outValue = new Text("");
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(filePath.indexOf("paper") != -1){
				outKey.set(infos[PaperStructure.first_organization]);
				context.write(outKey, outValue);
				for(int i=0;i<4;i++){
					context.write(new Text(infos[PaperStructure.first_author_unit + i*4]), outValue);
				}
			}
			if(filePath.indexOf("patent") != -1){
				outKey.set(infos[PatentStructure.applicant]);
				context.write(outKey, outValue);
			}
			if(filePath.indexOf("project") != -1){
				outKey.set(infos[ProjectStructure.unit]);
				context.write(outKey, outValue);
				for(int i=0;i<4;i++){
					context.write(new Text(infos[ProjectStructure.leader_unit + 2*i]), outValue);
				}
			}
		} 
	}
	
	public static class ExportAllUnitReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			if(!key.toString().equals("null") && !key.toString().equals("")){
				context.write(key, NullWritable.get());
			}
		}
	}
}
