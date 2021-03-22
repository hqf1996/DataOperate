package com.other;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PatentStructure;

public class ExportSomePatent {
	public static class ExportSomePatentMap extends Mapper<Object, Text, Text, NullWritable>{
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[PatentStructure.subject_code].contains("电子信息") || infos[PatentStructure.subject_code].contains("机械电子与制造")
			   || infos[PatentStructure.subject_code].contains("化学化工") || infos[PatentStructure.subject_code].contains("生物医药")){
				if(infos[PatentStructure.name].length() < 8 || !isChinese(infos[PatentStructure.name].substring(infos[PatentStructure.name].length()-1))){
					context.write(new Text(infos[PatentStructure.PATENT_ID] + "\t" + infos[PatentStructure.name] + "\t" + infos[PatentStructure.publication_no]), NullWritable.get());
				}
			}
		}
		
		private boolean isChinese(String str){
			if (str == null) {
	            return false;
	        }
	        Pattern pattern = Pattern.compile("[\\u4E00-\\u9FBF]+");
	        return pattern.matcher(str.trim()).find();
		}
	}
}
