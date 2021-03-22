package com.updateTableUnit;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import com.structure.ExpertStructure;

/**
 * 提取出新增的专家
 * 数据来源：step4和科技厅导入的专家数据
 * 数据产生：add_expert
 * @author yhj
 */
public class AchievementUnitAddExpert {

	public static class AchievementUnitAddExpertMap extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 7){
				context.write(new Text(infos[0]), new Text(infos[0] + "\t" + infos[1] + "\t" + infos[2] + "\t" + infos[6]));
			}
			if(infos.length == ExpertStructure.totalNum){
				context.write(new Text(infos[ExpertStructure.EXPERT_ID]), new Text(infos[ExpertStructure.EXPERT_ID]));
			}
		}
	}
	
	public static class AchievementUnitAddExpertReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] addInfos = null;
			Boolean flag = true;
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 4){
					addInfos = infos;
				}
				if(infos.length == 1){
					flag = false;
					break;
				}
			}
			if(addInfos != null && flag){ //已经在专家表中的元素不再输出
				String[] result = new String[51];
				for(int i= 0;i<result.length;i++){
					result[i] = "null";
				} 
				result[1] = addInfos[0];
				result[2] = addInfos[1];
				result[27] = addInfos[2];
				result[50] = addInfos[3];
				if(!(result[1].contains("课题组") || result[1].contains("编辑部") || result[1].contains("项目组") || result[1].contains("不公开姓名") || result[1].contains("发明人"))){
					context.write(new Text(StringUtils.join(result, "\t").replace("'", "")), NullWritable.get());
				}
			}
		}
	}
}
