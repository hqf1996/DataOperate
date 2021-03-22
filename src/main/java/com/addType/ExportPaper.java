package com.addType;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PaperStructure;

public class ExportPaper {
	
	public static class ExportPaperMap extends Mapper<Object, Text, Text, NullWritable>{
		int[] lengthLimit = {10, 50, 255, 1024, 255, 1024, 1024, 1024, 255, 1024,
				10, 255, 255, 1024, 10, 255, 255, 1024, 10, 255,
				255, 1024, 10, 255, 1024, 4096, 255, 255, 255, 255,
				255, 255, 255, 255, 255, 255, 255, 255, 50, 255,
				255, 10, 10};
		Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String[] result = new String[PaperStructure.totalNum];
			for(int i = 0;i<result.length;i++){
				if(infos[i].length() > lengthLimit[i]){
					result[i] = infos[i].substring(0, lengthLimit[i]);
				}else {
					result[i] = infos[i];
				}
			}
			outKey.set(StringUtils.join(result,"\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
