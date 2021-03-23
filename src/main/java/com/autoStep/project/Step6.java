package com.autoStep.project;

import com.structure.ProjectStructure;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 提取出增加的项目
 * 数据来源：step5
 * 跟新时 数据产生：project_new
 * 添加时 数据产生：add_project或project_new
 * @author yhj
 *
 */
public class Step6 {
	public static class Step6Map extends Mapper<Object, Text, Text, NullWritable>{
		int[] lengthLimit = {10, 50, 255, 1024, 255, 1024, 255, 1024, 255, 1024,
				255, 1024, 1024, 1024, 255, 255, 255, 255, 4096, 4096,
				4096, 4096, 255, 255, 255, 255, 255, 255, 255, 50,
				10, 10, 10};
		Text outKey = new Text();
		// 处理下数据长度，当超出限制时进行截取
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String[] result = new String[ProjectStructure.totalNum];
			for(int i = 0; i< ProjectStructure.totalNum; i++){
				if(infos[i].length() > lengthLimit[i]){
					result[i] = infos[i].substring(0, lengthLimit[i]);
				}else {
					result[i] = infos[i];
				}
			}
			outKey.set(StringUtils.join(result, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
