package com.autoStep.project;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ProjectStructure;

/**
 * step5 在项目字段最后添加专家的id
 * 输入 step2和step4的输出
 * @author yhj
 */
public class Step5 {

	public static class Step5Map extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 7){
				context.write(new Text(infos[3]), value);
			}
			if(infos.length >= ProjectStructure.totalNum){
				context.write(new Text(infos[ProjectStructure.PROJECT_ID]), value);
			}
		}
	}
	
	public static class Step5Reduce extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] projectInfos = null;
			String[] expertIds = {"null", "null", "null", "null"};
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 7){
					int expertRole = Integer.parseInt(infos[5]);
					expertIds[expertRole-1] = infos[0]; 
				}
				if(infos.length > ProjectStructure.totalNum){
					projectInfos = infos;
				}
			}
			if(projectInfos != null){
				if(projectInfos.length < (ProjectStructure.fourth_author_f+1)){
					projectInfos = Arrays.copyOf(projectInfos, ProjectStructure.fourth_author_f+1);
				}
				for(int i = 0;i<expertIds.length;i++){
					projectInfos[ProjectStructure.first_author_f+i] = expertIds[i];
				}
				outKey.set(StringUtils.join(projectInfos, "\t"));
				System.out.println(outKey.toString());
				context.write(outKey, NullWritable.get());
			}
		}
	}
}
