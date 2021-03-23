package com.autoStep.project;

import com.structure.ExpertStructure;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 提取出新增的专家
 * 数据来源：step4和/user/mysqlOut/expert
 * @author yhj
 *
 */
public class Step8 {

	public static class Step8Map extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			// 如果是step4输出的文件
			// expertId、expertName、expertUnit、projectId、projectName、
			// expertRole、source
			if(infos.length == 7){
				context.write(new Text(infos[0]), new Text(infos[0] + "\t" + infos[1] + "\t" + infos[2] + "\t" + infos[6]));
			}
			// 如果是expert表，
			if(infos.length == ExpertStructure.totalNum){
				context.write(new Text(infos[ExpertStructure.EXPERT_ID]), new Text(infos[ExpertStructure.EXPERT_ID]));
			}
		}
	}
	
	public static class Step8Reduce extends Reducer<Object, Text, Text, NullWritable>{
		@Override
		protected void reduce(Object key, Iterable<Text> values, Reducer<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] addInfos = null;
			Boolean flag = true;
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 4){
					addInfos = infos;
				}
				// 说明expert存在该专家，
				if(infos.length == 1){
					flag = false;
					break;
				}
			}
			if(addInfos != null && flag){ //已经在专家表中的元素不再输出
				String[] result = new String[52];
				for(int i= 0;i<result.length;i++){
					result[i] = "null";
				} 
				result[1] = addInfos[0];
				result[2] = addInfos[1];
				result[27] = addInfos[2];
				result[50] = addInfos[3];
				if(result[1].indexOf("编辑部")== -1 && result[1].indexOf("发明人")== -1){
					context.write(new Text(StringUtils.join(result, "\t").replace("'", "")), NullWritable.get());
				}
			}
		}
	}
}
