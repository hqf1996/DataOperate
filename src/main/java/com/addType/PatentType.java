package com.addType;

import java.io.IOException;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PatentStructure;

public class PatentType {
	
	public static class AddPatentTypeMap extends Mapper<Object, Text, Text, NullWritable>{
		HashSet<Character> typeSet = new HashSet<Character>();
		String application_no = null;
		String type = null;
		Text outKey = new Text();
		@Override 
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			typeSet.add('1');typeSet.add('2');typeSet.add('3');
			typeSet.add('4');typeSet.add('8');typeSet.add('9');
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			application_no = infos[PatentStructure.application_no];
			if(typeSet.contains(application_no.charAt(6))){
				type = String.valueOf(application_no.charAt(6));
			}else {
				type = "4";
			}
			infos[0] = "null";
			infos[PatentStructure.url] = infos[PatentStructure.url] + "\t" + type + "\t" + infos[PatentStructure.url+3];
			outKey.set(StringUtils.join(infos, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
