package com.zhanglb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 关联领域和单位找专拣的表生成领域找专家的表
 * 输入数据 来自step2和step5产生的结果
 * step6
 * @author yhj
 */
public class FinalJoin {
	public static class FinalJoinMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 2){
				outKey.set(infos[0]);
				context.write(outKey, value);
			}
			if(infos.length == 12){
				outKey.set(infos[0]);
				context.write(outKey, value);
			}
		}
	}
	
	
	public static class FinalJoinReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Text outKey = new Text();
		public Text outValue = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String> list = new ArrayList<String>();
			String result = null;
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 2){
					list.add(infos[1].replace(" ", "\t"));
				}
				if(infos.length == 12){
					result = StringUtils.join(infos, "\t");
				}
			}
			if(!list.isEmpty() && result != null){
				for(String s:list){
					String[] infos = s.split("\t");
					if(infos.length == 5){
						String keyOut = "null\t" + result + "\t" + s;
						System.out.println(keyOut);
						outKey.set(keyOut);
						context.write(outKey, NullWritable.get());
					}
				}
			}
		}
	}
}
