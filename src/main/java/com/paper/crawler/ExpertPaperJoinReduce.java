package com.paper.crawler;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExpertPaperJoinReduce extends Reducer<Text, Text, Text, Text>{
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String paperData = null;
		String expertUnit[] = new String[8];//单数是作者的名字，双数是专家在百度学术中的查找情况和单位（格式：查找情况'单位）
		HashMap<String, String> crawlerExpertUnitMap = new HashMap<>();
		for(Text value: values){
			String data = value.toString();
			String infos[] = data.split("'");
			if(infos.length == 33){
				paperData = data;
				expertUnit[0] = infos[8];
				expertUnit[2] = infos[10];
				expertUnit[4] = infos[12];
				expertUnit[6] = infos[14];
				expertUnit[1] = expertUnit[3] = expertUnit[5] = expertUnit[7] = infos[6]+ "'99";//默认没有找到，单位为第一作者的单位
			}
			if(infos.length == 7){
				crawlerExpertUnitMap.put(infos[2], infos[4]+ "'" + infos[3]);
			}
		}
		if(paperData != null){
			if(!crawlerExpertUnitMap.isEmpty()){
				for(int i = 0;i < 8;i = i+2){
					if(crawlerExpertUnitMap.containsKey(expertUnit[i])){
						expertUnit[i+1] = crawlerExpertUnitMap.get(expertUnit[i]);  
					}
				}
			}
			String result = paperData + "'" + StringUtils.join(expertUnit, "'");
			context.write(new Text(result), new Text(""));
		}
	}
}
