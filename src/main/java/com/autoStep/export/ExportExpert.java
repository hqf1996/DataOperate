package com.autoStep.export;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ExpertStructure;

/**
 * 
 * @author yhj
 * 导出专家表中的专家名字，为姓名找人提供联想词袋
 */
public class ExportExpert {

	public static class ExportExpertMap extends Mapper<Object, Text, Text, Text>{
		static final int STRNUM = ExpertStructure.totalNum; //表的长度
		static final int STRP = ExpertStructure.name; // 提取的字段所在的位置
		public Text outKey = new Text();
		public Text outValue = new Text("");
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
//			if(infos.length == STRNUM && infos[STRP].length() >= 4 && !infos[STRP].equals("null")){
//				unit.set(infos[STRP]);
//				System.out.println(unit.toString());
//				context.write(unit, outValue);
//			}
			if(infos.length == STRNUM && !infos[STRP].equals("null") && !infos[STRP].equals("") && infos[STRP].length() >= 2 && infos[STRP].length() <= 6){
				String pattern = "^\\(.*?\\)|^(\\d)*\\.|^\\d|^[?（：￥\\s]";
				Pattern r = Pattern.compile(pattern);
				Matcher m = r.matcher(infos[STRP]);
				if(!m.find()){
					outKey.set(infos[STRP]);
//					System.out.println(outKey.toString());
					context.write(outKey, outValue);
				}
			}
		}
	}
	
	public static class ExportExpertReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}
	}
}
