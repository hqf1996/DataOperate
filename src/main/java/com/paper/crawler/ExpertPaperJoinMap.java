package com.paper.crawler;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExpertPaperJoinMap extends Mapper<Object,Text, Text, Text>{
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String data = value.toString().replace("\t", "");
		String[] infos = data.split("'");
		if (infos.length >= 7){
			context.write(new Text(infos[1]), new Text(data));
		}
	}
}
