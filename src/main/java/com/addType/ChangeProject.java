package com.addType;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.ProjectStructure;
public class ChangeProject {
	
	public static class ChangeProjectMap extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] info = value.toString().split("\t");
			info[ProjectStructure.unit] = info[ProjectStructure.unit_f];
			outKey.set(StringUtils.join(info, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
