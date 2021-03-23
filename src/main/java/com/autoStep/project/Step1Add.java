package com.autoStep.project;

import com.structure.ProjectStructure;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.UUID;

/**
 * 增加的新数据,和原来的项目数据去重,添加uuid
 * 数据来源： Pretreatment*和mysqlOut/project_new_unit_subject_code_join_all_expert
 * 数据产生：step1
 * @author yhj
 *
 */
public class Step1Add {
	public static class Step1Map extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			infos[ProjectStructure.name] = infos[ProjectStructure.name].trim();
			infos[ProjectStructure.leader] = infos[ProjectStructure.leader].trim();
			outKey.set(infos[ProjectStructure.name] + infos[ProjectStructure.leader]);
			context.write(outKey, value);
		}
	}
	
	public static class Step1Reduce extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			boolean isNewData = true;
			String[] result = null;
			// 判断是否为新的数据
			// 新数据和之前的数据长度不一样，
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == (ProjectStructure.fourth_author_f+1)){
					isNewData = false;
					break;
				}
				if(infos.length == (ProjectStructure.fourth_author_f-3)){
					result = infos;
				}
			}
			// 当为新数据时，写文件，不是新数据，直接退出
			if(isNewData && result != null ){
				result[ProjectStructure.PROJECT_ID] = UUID.randomUUID().toString();
				outKey.set(StringUtils.join(result, "\t"));
				context.write(outKey, NullWritable.get());
			}	
		}
	}
}
