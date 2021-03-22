package com;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.ExpertPaperJoinStructure;
import com.structure.PaperStructure;

/**
 * 为雷达图的计算提供临时表
 * 表的数据来源：/user/mysqlOut/paper和/user/mysqlOut/expert_paper_join
 * @author yhj
 *
 */
public class CreateTmpTable {
	public static class CreateTmpTableMap extends Mapper<Object, Text, Text, Text>{
		Text keyOut = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			//将论文的id作为key
			if(infos.length == ExpertPaperJoinStructure.totalNum){
				keyOut.set(infos[ExpertPaperJoinStructure.PAPER_ID]);
				context.write(keyOut, value);
			}
			if(infos.length == PaperStructure.totalNum){
				keyOut.set(infos[PaperStructure.PAPER_ID]);
				context.write(keyOut, value);
			}
		}
	}
	
	public static class CreateTmpTableReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text keyOut = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] paper = null;
			List<String[]> expertPaperJoinList = new ArrayList<>();
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == PaperStructure.totalNum){
					paper = infos;
				}
				if(infos.length == ExpertPaperJoinStructure.totalNum){
					expertPaperJoinList.add(infos);
				}
			}
			if(paper != null && !expertPaperJoinList.isEmpty()){
				for(String[] expertPaperJoin: expertPaperJoinList){
					String[] result = Arrays.copyOfRange(expertPaperJoin, 1, expertPaperJoin.length-1);
					keyOut.set(StringUtils.join(result, "\t") + "\t" + paper[PaperStructure.first_author] + "\t" 
							+ paper[PaperStructure.authors] + "\t" + paper[PaperStructure.journal_name] + "\t" + paper[PaperStructure.year] + "\t");
					context.write(keyOut, NullWritable.get());
				}
			}
		}
	}
}
