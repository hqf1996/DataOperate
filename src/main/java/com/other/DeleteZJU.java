package com.other;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.ProjectStructure;

public class DeleteZJU {
	public static class DeleteZJUMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[ProjectStructure.leader].equals("浙江大学")){
				infos[ProjectStructure.leader] = "null";
				infos[ProjectStructure.leader_unit] = "null";
				infos[ProjectStructure.first_author_f] = "null";
				infos[ProjectStructure.first_author_rank_code] = "null";
				System.out.println(value.toString());
				context.write(new Text(StringUtils.join(infos, "\t")), NullWritable.get());
			}else {
				context.write(value, NullWritable.get());
			}
		}
	}
}
