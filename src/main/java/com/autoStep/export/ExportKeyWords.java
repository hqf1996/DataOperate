package com.autoStep.export;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.util.Util;

/**
 * 
 * @author yhj
 * 导出论文表中的关键字(要选好期刊的论文)
 */
public class ExportKeyWords {

	public static class ExportKeyWordsMap extends Mapper<Object, Text, Text, NullWritable>{
		public Text outKey = new Text();
		public Map<String, String> journalMap = new HashMap<String, String>(); 
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			journalMap = Util.getMapFromDir("paper_journal", 0, 0);
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String journal = infos[36];
			if(!infos[24].equals("null")){
				String[] keyWords = infos[24].split(";|/|,");
				if(journalMap.containsKey(journal)){
					for(int i = 0;i<keyWords.length;i++){
						if(!keyWords[i].equals("")){
							outKey.set(keyWords[i]);
							System.out.println(infos[1] + "\t" + infos[24]);
							System.out.println(outKey.toString());
							context.write(outKey, NullWritable.get());
						}
					}
				}else {
					System.out.println(value.toString());
				}
			}
		}
	}
	
	
	public static class ExportKeyWordsReduce extends Reducer<Text, NullWritable, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
		}
	}
}
