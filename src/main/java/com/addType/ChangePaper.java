package com.addType;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PaperStructure;

public class ChangePaper {

	public static class ChangePaperMap extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			infos[PaperStructure.id] = "null";
			outKey.set(StringUtils.join(infos, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
