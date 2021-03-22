package com.autoStep.export.countExpert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author yhj
 * Step2之后的一个步骤，加上张师兄的文件，join一下
 */
public class CountAllExpertBaseUnitZhang {

	public static class CountAllExpertBaseUnitZhangMap extends Mapper<Object, Text, Text, Text>{
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
			if(infos.length == 11){
				outKey.set(infos[0]);
				context.write(outKey, value);
			}
		}
	}
	
	
	public static class CountAllExpertBaseUnitZhangReduce extends Reducer<Text, Text, Text, NullWritable>{
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
				if(infos.length == 11){
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
