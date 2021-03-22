package com.updateTableUnit;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;

/**
 * step5 给成果表补上对应的专家id
 * step4 和 step1
 * @author yhj
 *
 */
public class AchievementExpertJoin {
	public static class AchievementExpertJoinMap extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 7 && infos[6].charAt(0) == '2'){
				context.write(new Text(infos[3]), value);
			}
			if(infos.length ==  ProjectStructure.fourth_author_f-3){
				context.write(new Text(infos[1]), value);
			}
		}
	}
	
	public static class AchievementExpertJoinReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] paperInfos = null;
			String[] expertIds = {"null", "null", "null", "null"};
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 7){
					int expertRole = Integer.parseInt(infos[5]);
					expertIds[expertRole-1] = infos[0]; 
				}
				if(infos.length ==  ProjectStructure.fourth_author_f-3){
					paperInfos = infos;
				}
			}
			if(paperInfos != null){
				String result = StringUtils.join(paperInfos, "\t") + "\t" + StringUtils.join(expertIds, "\t");
				System.out.println(result);
				context.write(new Text(result), NullWritable.get());
			}
		}
	}
}
