package com.expert.extract;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrawlerPaperExtractExpertMap extends Mapper<Object, Text, Text, Text>{
	final int strsSum = 45;
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String strs[] = value.toString().replace("\t", "").split("'");
		if(strs.length == strsSum){
			int step = 3;
			int start = 33;
			String paperId = strs[1];
			String paperName = strs[2];
			for(int i = 0;i < 4;i++){
				int place = start+step*i;
				String name = strs[place];
				if(name.equals("null") || name.equals("")){//前面的作者为空说明后面的作者都没了，直接跳出循环
					break;
				}
				String unit = strs[place+1];
				String infoOfBaidu = strs[place+2];
				String uuid = UUID.randomUUID().toString();
				String source;
				if(i == 0){
					if(unit.equals("null") || unit.equals("")){//单位为空的第一作者
						source = SourceMessage.PAPERFIRSTNOUNIT;
					}else {
						source = SourceMessage.PAPERFIRSTUNIT;
					}
				}else {
					source = SourceMessage.PAPERNOFIRST;
				}
				//最后输出格式： 专家ID,论文ID，专家姓名，单位，第几作者，来源，来源（细分）
				String result = uuid + "'" + paperId + "'" + paperName + "'" + name + "'" + unit + "'" + (i+1) + "'" +SourceMessage.PAPER + "'" + source+ "'" + infoOfBaidu;
				System.out.println("----->Map out:" + result);
				context.write(new Text(result), new Text(""));
			}
		}
	}
}
