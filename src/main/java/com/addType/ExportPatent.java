package com.addType;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PatentStructure;

public class ExportPatent {
	
	public static class ExportPatentMap extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String[] result = new String[PatentStructure.totalNum];
			for(int i = 0;i<result.length;i++){
				result[i] = infos[i];
			}
			outKey.set(StringUtils.join(result,"\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
