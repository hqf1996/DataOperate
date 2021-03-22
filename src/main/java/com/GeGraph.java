package com;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 读取临时表数据来生成雷达图的数据
 * @author yhj
 *
 */
public class GeGraph {
	
	public static class GeGraphMap extends Mapper<Object, Text, Text, Text>{
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
	
	
	public static class GeGraphReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Text outKey = new Text();
		RadoComputingPaperOnHadoop radoComputingPaperOnHadoop;
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			radoComputingPaperOnHadoop = new RadoComputingPaperOnHadoop();
		} 
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<List<String>> paperList = new ArrayList<List<String>>();
			String[] infos = null;
			for(Text value:values){
				infos = value.toString().split("\t");
				List<String> p = new ArrayList<>();
				p.add(0, infos[4]);//paper表中的first_author字段
				p.add(1, infos[5]);//paper表中的authors字段
				p.add(2, infos[6]);//paper表中的journal_name字段
				if(infos.length <= 7){
					p.add(3, "");//paper表中的year字段
				}else {
					p.add(3, infos[7]);//paper表中的year字段
				}
				p.add(4, infos[2]);//paper表中的第几作者
				paperList.add(p);
			}
			if(!paperList.isEmpty()){
				double[] result = radoComputingPaperOnHadoop.PaperComputing(key.toString(), paperList);
				String out = infos[0] + ",\"" + infos[1] + "\"," + Double.toString(result[0]) + "\t" + "," + Double.toString(result[1]) + "\t";
				System.out.println(out);
				outKey.set(out);
				context.write(outKey, NullWritable.get());
			}
		} 
	}
}
