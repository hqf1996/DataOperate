package com.autoStep.paper;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {
	static String mapredJapPath;
	static String jobName;
	static Class<? extends Mapper<?, ?, ?, ?>> mapClass;
	static Class<? extends Reducer<?, ?, ?, ?>> reduceClass;
	static boolean isCombiner;
	static String fs;
	
	public static void init(){
		//mapredJapPath = "E:\\AutoDataProcess\\FindStep.jar";
		//jobName = "FindStep";
		//mapClass = FindStep.FindMap.class;
		reduceClass = null;
		isCombiner = false;
		fs = "hdfs://10.1.13.101:8020";

	}
	
	/**
	 * mapreduce的入口函数，一般需要修改程序的mapred.jar的位置、MapReduce类、数据输入输出的位置格式，每次修改需要打包一次
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		init();
		Configuration conf = new Configuration(); // 启用默认配置
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		if (otherArgs.length < 2){
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		conf.set("mapred.jar",mapredJapPath);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, jobName);
		job.addCacheFile(new URI(fs + "/user/mysqlOut/unit"));
		job.addCacheFile(new URI(fs + "/user/mysqlOut/unit_citycode_map"));
		job.addCacheFile(new URI(fs + "/user/mysqlOut/university_short_call"));
//		job.addCacheFile(new URI(fs + "/user/mysqlOut/university_change_name"));
		
//		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysqlOut/unit_areacode_map_new"));
//		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysqlOut/unit_citycode_map"));
//		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysqlOut/unit_provincecode_map"));
//		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysqlOut/china_universities"));
//		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysqlOut/china_institues"));
//		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysqlOut/subject_paper_map"));
//		job.addCacheFile(new URI("hdfs://10.1.13.111:8020/user/mysqlOut/paper_journal"));
		job.setJarByClass(Main.class);
		job.setMapperClass(mapClass);
		if(reduceClass != null){
			if(isCombiner){
				job.setCombinerClass(reduceClass);
			}
			job.setReducerClass(reduceClass);
		}
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		for(int i = 0;i < otherArgs.length-1;i++){
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
