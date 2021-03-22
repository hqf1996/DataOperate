package com.other;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.ProjectStructure;

public class ChangeProjectType {
	public static class ChangeProjectTypeMap extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[ProjectStructure.project_type_more].equals("211")){
				infos[ProjectStructure.project_type_more] = "116";
				infos[ProjectStructure.project_type] = "1";
				System.out.println(infos[ProjectStructure.PROJECT_ID]);
			}
			outKey.set(StringUtils.join(infos, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
