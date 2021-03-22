package com.expert.extract;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PatentExtractExpertMap extends Mapper<Object, Text, Text, Text>{
	final int strsSum = 25;
	final int idPlace = 1;
	final int namePlace = 2;
	final int unitPlace = 8;
	final int inventorsPlace = 10;
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String data = value.toString().replace("\t", "");
		System.out.println("----->Map in:"+ data);
		String strs[] = data.split("'");
		if(strs.length == strsSum){
			int step = 1;
			int start = 11;
			String patentId = strs[idPlace];
			String patentName = strs[namePlace];
			String unit = strs[unitPlace];
			if(unit.contains("·") || unit.equals("null") || unit.equals("") || unit.length() < 4){//专利的单位有很多是人名，需要特殊处理
				unit = "null";
			}else {
				String inventors = strs[inventorsPlace];
				unit = unit.split(";")[0];
				if(inventors.contains(unit)){
					unit = "null";
				}
			}
			for(int i = 0;i < 4;i++){
				int place = start+step*i;
				String expertName = strs[place];
				if(expertName.equals("null") || expertName.equals("")){//前面的专家为空，后面必定为空
					break;
				}
				String uuid = UUID.randomUUID().toString();
				String source;
				if(i == 0){
					source = SourceMessage.PATENTFIRST;
				}else {
					source = SourceMessage.PATENTNOFIRST;
				}
				//最后输出格式： 专家ID,论文ID，专家姓名，单位，第几作者，来源
				String result = uuid + "'" + patentId + "'" + patentName + "'" + expertName + "'" + unit + "'" + (i+1) + "'" +SourceMessage.PATENT + "'" + source;
				System.out.println("----->Map out:" + result);
				context.write(new Text(result), new Text(""));
			}
		}
	}
}
