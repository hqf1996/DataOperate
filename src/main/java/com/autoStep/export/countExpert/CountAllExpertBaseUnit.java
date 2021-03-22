package com.autoStep.export.countExpert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author yhj
 * Step2
 * 在CountExpertBaseUnitPaper,patent,project之后
 * 统计论文，专利。项目专家发了多少篇，主要领域在哪
 */
public class CountAllExpertBaseUnit {
 
	public static class CountAllExpertBaseUnitMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			outKey.set(infos[0]);
			context.write(outKey, value);
		}
	}
	
	public static class CountAllExpertBaseUnitReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Text outKey = new Text();
		Map<Integer, String> subjectIndexCodeMap = new HashMap<Integer, String>();
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			subjectIndexCodeMap.put(0, "A1");
			subjectIndexCodeMap.put(1, "A2");
			subjectIndexCodeMap.put(2, "A3");
			subjectIndexCodeMap.put(3, "A4");
			subjectIndexCodeMap.put(4, "A5");
			subjectIndexCodeMap.put(5, "A9");
			subjectIndexCodeMap.put(6, "C1");
			subjectIndexCodeMap.put(7, "C2");
			subjectIndexCodeMap.put(8, "Z1");
			subjectIndexCodeMap.put(9, "Z9");
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int[] oneSum = {0, 0, 0};
			int[] subjectCount = {0, 0, 0, 0, 0, 0, 0, 0, 0};
			int s = 0;
			String baidu = "null";
			String[] infos = null;
			for(Text value:values){
				infos = value.toString().split("\t");
				oneSum[0] += Integer.parseInt(infos[2]);
				oneSum[1] += Integer.parseInt(infos[3]);
				oneSum[2] += Integer.parseInt(infos[4]);
				for (int i = 0; i < subjectCount.length; i++) {
					subjectCount[i] += Integer.parseInt(infos[5+i]); 
				}
				if(baidu.equals("null") && !infos[15].equals("null")){
					baidu = infos[15];
				}
			}
			s = oneSum[0] + oneSum[1] + oneSum[2];
			
			int firstIndex = 9, secondIndex = 9, first = 0, second = 0;
			for(int i = 0;i<subjectCount.length;i++){
				if(subjectCount[i] > first){
					second = first;
					secondIndex = firstIndex;
					first = subjectCount[i];
					firstIndex = i;
				}else if (subjectCount[i] > second) {
					second = subjectCount[i];
					secondIndex = i;
				}
			}
			if(infos[1].length() >= 4){
				outKey.set(infos[0] + "\t" + infos[16] + "\t" + infos[1] + "\t" + infos[17] + "\t" + s + "\t" + oneSum[0] + "\t" + oneSum[1] + "\t" + oneSum[2] + "\t" + 
						subjectIndexCodeMap.get(firstIndex) + "\t" + subjectIndexCodeMap.get(secondIndex) + "\t" + baidu);
				context.write(outKey, NullWritable.get());
			}
		}
	}
}
