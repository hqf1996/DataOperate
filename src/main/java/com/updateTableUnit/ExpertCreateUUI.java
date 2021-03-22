package com.updateTableUnit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ExpertStructure;

/**
 * step4 给专家补上uuid
 * step2 和 step3 的输出
 * @author yhj
 *
 */
public class ExpertCreateUUI {

	public static class ExpertCreateUUIdMap extends Mapper<Object, Text, Text, Text>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 6){
				if(!(infos[0].contains("课题组") || infos[0].contains("编辑部") || infos[0].contains("项目组") || infos[0].contains("不公开姓名") || infos[0].contains("发明人"))){
					infos[0] = infos[0].replaceAll("^\\(.*?\\)|^(\\d)*\\.|^\\)", "");
					if(!infos[0].equals("")){
						context.write(new Text(infos[0]+infos[1]), value);
					}
				}
			}
			if(infos.length == ExpertStructure.totalNum){
				context.write(new Text(infos[ExpertStructure.name]+infos[ExpertStructure.unit]), new Text(infos[1]));
			}
		}
	}
	
	public static class ExpertCreateUUIdReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> list = new ArrayList<>();
			String expertUUid = null;
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 6){
					list.add(infos);
				}else {
					if(infos.length ==1 ){
						expertUUid = infos[0];
					}
				}
			}
			if(expertUUid == null){
				String newUUId = UUID.randomUUID().toString();
				for(String[] infos:list){
						String result = newUUId + "\t" + StringUtils.join(infos, "\t");
						System.out.println("create a new uuid :" + result);
						context.write(new Text(result), NullWritable.get());
				}
			}else {
				for(String[] infos:list){
						String result = expertUUid + "\t" + StringUtils.join(infos, "\t");
						System.out.println("uuin from expert table :" + result);
						context.write(new Text(result), NullWritable.get());
				}
			}
		}
	}
}
