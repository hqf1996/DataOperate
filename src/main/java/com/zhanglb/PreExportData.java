package com.zhanglb;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;

/**
 * 产生领域找人处理的数据
 * step1
 * @author yhj
 *
 */
public class PreExportData {
	
	/**
	 * 论文
	 * /user/mysqlOut          /user/offlineCalculationData/yuan/cut_add_merge_words/paper
	 * 输入 paper_all_expert  和  paper的分词
	 * @author yhj
	 */
	public static class PreExportPaperDataMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(filePath.contains("cut")){
				String[] cutInfos = value.toString().split(" ", 2);
				if(cutInfos.length >=2){
					outKey.set(cutInfos[0]);
					outValue.set("cut\t" + cutInfos[1]);
					context.write(outKey, outValue);
				}
			}else {
				String[] infos = value.toString().split("\t");
//				if(infos.length < PaperStructure.totalNum){
//					context.setStatus(value.toString());
//					System.out.println("Error : " + value.toString());
//				}else {
				boolean flag = false;
				String result = infos[PaperStructure.PAPER_ID];
				for(int i = 0;i<4;i++){
					if(!infos[PaperStructure.first_author_f + i].equals("null")){
						if(!flag){
							flag = true;
							result = result + "   " + infos[PaperStructure.first_author_f + i] + " " + infos[PaperStructure.first_author_baidu + i*4];
						}else {
							result = result + " " + infos[PaperStructure.first_author_f + i] + " " + infos[PaperStructure.first_author_baidu + i*4];
						}
					}
				}
				if(flag){
					outKey.set(infos[PaperStructure.PAPER_ID]);
					outValue.set("experts\t" + result);
					context.write(outKey, outValue);
				}
//				}
			}
		}
	}
	
	/**
	 * 专利
	 * 输入 patent_all_expert 和  patent的分词
	 * @author yhj
	 */
	public static class PreExportPatentDataMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(filePath.contains("cut")){
				String[] cutInfos = value.toString().split(" ", 2);
				if(cutInfos.length >=2){
					outKey.set(cutInfos[0]);
					outValue.set("cut\t" + cutInfos[1]);
					context.write(outKey, outValue);
				}
			}else {
				String[] infos = value.toString().split("\t");
				boolean flag = false;
				String result = infos[PatentStructure.PATENT_ID];
				for(int i = 0;i<4;i++){
					if(!infos[PatentStructure.first_author_f + i].equals("null")){
						if(!flag){
							flag = true;
							result = result + "   " + infos[PatentStructure.first_author_f + i] + " " + "null";
						}else {
							result = result + " " + infos[PatentStructure.first_author_f + i] + " " + "null";
						}
					}
				}
				if(flag){
					outKey.set(infos[PatentStructure.PATENT_ID]);
					outValue.set("experts\t" + result);
					context.write(outKey, outValue);
				}
			}
		}
	}
	
	/**
	 * 项目
	 * 输入 project_all_expert 和  project的分词
	 * @author yhj
	 *
	 */
	public static class PreExportProjectDataMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(filePath.contains("cut")){
				String[] cutInfos = value.toString().split(" ", 2);
				if(cutInfos.length >=2){
					outKey.set(cutInfos[0]);
					outValue.set("cut\t" + cutInfos[1]);
					context.write(outKey, outValue);
				}
			}else {
				String[] infos = value.toString().split("\t");
				boolean flag = false;
				String result = infos[ProjectStructure.PROJECT_ID];
				for(int i = 0;i<4;i++){
					if(!infos[ProjectStructure.first_author_f + i].equals("null")){
						if(!flag){
							flag = true;
							result = result + "   " + infos[ProjectStructure.first_author_f + i] + " " + "null";
						}else {
							result = result + " " + infos[ProjectStructure.first_author_f + i] + " " + "null";
						}
					}
				}
				if(flag){
					outKey.set(infos[ProjectStructure.PROJECT_ID]);
					outValue.set("experts\t" + result);
					context.write(outKey, outValue);
				}
			}
		}
	}
	
	/**
	 * 关联分词
	 * @author yhj
	 *
	 */
	public static class PreExportDataReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String experts = null;
			String cutInfos = null;
			for(Text value:values){
				String[] infos = value.toString().split("\t", 2);
				if (infos[0].equals("cut")) {
					cutInfos = infos[1];
				}
				if(infos[0].equals("experts")){
					experts = infos[1];
				}
			}
			if(experts != null && cutInfos != null){
				outKey.set(experts + "   " + cutInfos);
				context.write(outKey, NullWritable.get());
			}
		}
	}
	
}
