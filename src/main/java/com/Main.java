package com;

import com.autoStep.export.ExportPaperFeatureMap;
import com.autoStep.export.ExportPatentFeatureMap;
import com.autoStep.export.ExportProjectFeatureMap;
import com.util.JobDefaultInit;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


public class Main extends Configured implements Tool{
	static String mapredJapPath;
	static String jobName;
	static Class<? extends Mapper<?, ?, ?, ?>> mapClass;
	static Class<? extends Reducer<?, ?, ?, ?>> combinerClass;
	static Class<? extends Reducer<?, ?, ?, ?>> reduceClass;
	static boolean isCombiner;
	static String fs;
	
	public static void init(){
		mapredJapPath = "E:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar";
		jobName = "export project feature";
		mapClass = ExportProjectFeatureMap.class;
		reduceClass =  null;
		combinerClass = null;
		isCombiner = false;
		fs = "hdfs://10.1.13.111:8020";
	}

	@Override
	public int run(String[] args) throws Exception {
		init();
		Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
				mapredJapPath, args);
//		job.setNumReduceTasks(5);
		job.setMapperClass(mapClass);
		job.addCacheFile(new URI(fs + "/user/mysqlOut/paper_journal"));
		if(reduceClass != null){
			if(isCombiner){
				if(combinerClass != null){
					job.setCombinerClass(combinerClass);
				}else {
					job.setCombinerClass(reduceClass);
				}
			}
			job.setReducerClass(reduceClass);
		}

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * mapreduce的入口函数，一般需要修改程序的mapred.jar的位置、MapReduce类、数据输入输出的位置格式，每次修改需要打包一次
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Main(), args));
	}
}
