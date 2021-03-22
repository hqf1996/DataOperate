package com.updateTableUnit;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExportExpertJoinTable {
	public static class ExportExpertJoinTableMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[6].charAt(0) == '2'){ //paper是4，patent是3，project是2
				if(infos[1].length() > 255){
					infos[1] = infos[1].substring(0, 255);
				}
				if(infos[4].length() > 255){
					infos[4] = infos[4].substring(0, 255);
				}
				String result = "null" + "\t" + infos[0] + "\t" + infos[1] + "\t" + infos[5] + "\t" + infos[3] + "\t" + infos[4];
				context.write(new Text(result.replace("'", "")), NullWritable.get());
			}
		}
	}
}
