package com.graphSearch.preWork;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.util.Util;

public class EdgeRelateEntity {
	public static class EdgeRelateEntityMap extends Mapper<Object, Text, Text, Text>{
		Map<String, String> relationMap = null;
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			relationMap = Util.getMapFromDir("edge", "\t", 0, 1);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == 6){
				infos[4] = relationMap.get(infos[4]);
				context.write(new Text(), new Text());
			}
		}
	}
}
