package com.autoStep.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.util.Util;

/**
 * 
 * @author yhj
 * 按单位统计论文，按论文期刊的影响因子排序
 */
public class CountPaperBaseUnit {
	
	public static class CountPaperBaseUnitMap extends Mapper<Object, Text, Text, Text>{
		public Map<String, String> journalFactorMap = new HashMap<>();

		public Text outKey = new Text();
		public Text outValue = new Text();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			journalFactorMap = Util.getMapFromDir("journal_information_all", "\t",0, 1);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if (!infos[6].equals("null") && !infos[6].equals("	") && !infos[6].equals("")) {
				String journalName = infos[36];
				String factor = "0";
				if (journalFactorMap.containsKey(journalName)) {
					factor = journalFactorMap.get(journalName);
				}
				outKey.set(infos[6]);
				outValue.set(infos[1] + "\t" + factor);
				context.write(outKey, outValue);
			}
		}
	}
	
	
	public static class Paper implements Comparable<Paper>{
		public String paperId;
		public float factor;
		public Paper(String paperId, float factor) {
			this.paperId = paperId;
			this.factor = factor;
		}
		@Override
		public int compareTo(Paper o) {
			// TODO Auto-generated method stub
			if(this.factor > o.factor){
				return -1;
			}else if (this.factor < o.factor) {
				return 1;
			}
			return 0;
		}
		
	}
	
	public static class CountPaperBaseUnitReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<Paper> list = new ArrayList<Paper>();
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				list.add(new Paper(infos[0], Float.parseFloat(infos[1])));
			}
			Collections.sort(list);
			StringBuilder s = new StringBuilder(key.toString());
			for(Paper paper: list){
				s.append("\t");
				s.append(paper.paperId);
				s.append("\t");
				s.append(paper.factor);
			}
			outKey.set(s.toString());
			context.write(outKey, NullWritable.get());
		}
	}
}
