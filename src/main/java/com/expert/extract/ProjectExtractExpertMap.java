package com.expert.extract;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 从项目表抽取出专家
 * @author yhj
 *
 */
public class ProjectExtractExpertMap extends Mapper<Object, Text, Text, Text>{
	final int strsSum = 25;
	final int idPlace = 1;
	final int namePlace = 2;
	final int unitPlace = 8;
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String strs[] = value.toString().replace("\t", "").split("'");
		if(strs.length == strsSum){
			int step = 1;
			int start = 4;
			String projectId = strs[idPlace];
			String ProjectName = strs[namePlace];
			String unit = strs[unitPlace];
			for(int i = 0;i < 4;i++){
				int place = start+i*step;
				String expertName = strs[place];
				if(expertName.equals("null") || expertName.equals("")){
					break;
				}
				String uuid = UUID.randomUUID().toString();
				String source;
				if(i == 0){
					source = SourceMessage.PROJECTFIRST;
				}else {
					source = SourceMessage.PROJECTNOFIRST;
				}
				String result = uuid + "'" + projectId + "'" + ProjectName + "'" + expertName + "'" + unit + "'" + (i+1) + "'" +SourceMessage.PROJECT + "'" + source;
				System.out.println("----->Map out:" + result);
				context.write(new Text(result), new Text(""));
			}
		}
	}
}
