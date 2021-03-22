package com.autoStep.export;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PaperStructure;
import com.structure.PatentStructure;

/**
 * 
 * @author yhj
 * 导出语料
 */
public class ExportCorpus {

	/**
	 * 
	 * @author yhj
	 * 导出论文语料
	 */
	public static class ExportPaperCorpusMap extends Mapper<Object, Text, Text, NullWritable>{
		public Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[PaperStructure.Abstract].equals("null")){
				outKey.set(infos[1] +"," + infos[2]);
			}else {
				outKey.set(infos[1] +"," + infos[2] + " " + infos[PaperStructure.Abstract]);
			}
			context.write(outKey, NullWritable.get());
		}
	}
	
	/**
	 * 
	 * @author yhj
	 * 导出专利的语料
	 */
	public static class ExportPatentCorpusMap extends Mapper<Object, Text, Text, NullWritable>{
		public Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[PatentStructure.Abstract].equals("null")){
				outKey.set(infos[1] + "," + infos[2]);
			}else {
				outKey.set(infos[1] + "," + infos[2] + " " + infos[PatentStructure.Abstract]);
			}
			context.write(outKey, NullWritable.get());
		}
	}
	 
	/**
	 * 
	 * @author yhj
	 * 导出项目语料
	 */
	public static class ExportProjectCorpusMap extends Mapper<Object, Text, Text, NullWritable>{
		public Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			outKey.set(infos[1] + "," + infos[2]);
			context.write(outKey, NullWritable.get());
		}
	}
}
