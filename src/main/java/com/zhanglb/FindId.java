package com.zhanglb;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindId {

	public static class FindIdMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("   ")[1].split(" ");
			for(int i = 0;i<infos.length;i = i+2){
				if(infos[i].equals("e294bcac-b01d-48b3-9864-25f9a650d285")){
					context.write(value, NullWritable.get());
					break;
				}
			}
		}
	}
}
