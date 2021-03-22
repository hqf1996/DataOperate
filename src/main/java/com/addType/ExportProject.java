package com.addType;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.ProjectStructure;

public class ExportProject {

	public static class ExportProjectMap extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String[] result = new String[ProjectStructure.totalNum];
			for(int i = 0;i<result.length;i++){
				result[i] = infos[i];
			}
			outKey.set(StringUtils.join(result, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
