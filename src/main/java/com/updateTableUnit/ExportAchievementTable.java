package com.updateTableUnit;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;

public class ExportAchievementTable {
	public static class ExportAchievementTableMap extends Mapper<Object, Text, Text, NullWritable>{
// project
		int[] lengthLimit = {10, 50, 255, 1024, 255, 1024, 255, 1024, 255, 1024,
				255, 1024, 1024, 1024, 255, 255, 255, 255, 4096, 4096,
				4096, 4096, 255, 255, 255, 255, 255, 255, 255, 50,
				10, 10, 10};
// patent
//		int[] lengthLimit = {10, 50, 255, 255, 255, 255, 255, 255, 1024, 1024,
//				1024, 1024, 255, 1024, 255, 1024, 255, 1024, 255, 1024,
//				4096, 4096, 255, 1024, 255, 255, 255, 255, 255, 255,
//				255, 10, 10};
//paper
//		int[] lengthLimit = {10, 50, 255, 1024, 255, 1024, 1024, 1024, 255, 1024,
//				10, 255, 255, 1024, 10, 255, 255, 1024, 10, 255,
//				255, 1024, 10, 255, 1024, 4096, 255, 255, 255, 255,
//				255, 255, 255, 255, 255, 255, 255, 255, 50, 255,
//				255, 10, 10};
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String[] result = new String[ProjectStructure.totalNum];
			for(int i = 0;i<result.length;i++){
				if(infos[i].length() > lengthLimit[i]){
					result[i] = infos[i].substring(0, lengthLimit[i]);
				}else {
					result[i] = infos[i];
				}
			}
			context.write(new Text(StringUtils.join(result, "\t").replace("'", "")), NullWritable.get());
		}
	}
}
