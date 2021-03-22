package com.autoStep.export;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PatentStructure;

public class ChangeOneStr {
	public static class ChangeOneStrMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[PatentStructure.subject_code].equals("null") || infos[PatentStructure.subject_code].equals("Z9")){
				infos[PatentStructure.subject_code] = "其他";
			}
			context.write(new Text(StringUtils.join(infos, "\t")), NullWritable.get());
		}
	}
}
