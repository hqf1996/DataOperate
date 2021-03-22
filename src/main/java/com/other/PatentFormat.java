package com.other;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PatentStructure;

public class PatentFormat {
	public static class PatentFormatMap extends Mapper<Object, Text, Text, NullWritable>{
		Text keyOut = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			for(int i = 0;i<4;i++){
				infos[PatentStructure.inventor + i] = infos[PatentStructure.inventor + i] + "\tnull";
			}
			keyOut.set(StringUtils.join(Arrays.copyOf(infos, infos.length-4), "\t"));
			context.write(keyOut, NullWritable.get());
		}
	}
}
