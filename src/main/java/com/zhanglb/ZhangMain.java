package com.zhanglb;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class ZhangMain {
	static String mapredJapPath;
	static String jobName;
	static Class<? extends Mapper<?, ?, ?, ?>> mapClass;
	static Class<? extends Reducer<?, ?, ?, ?>> combinerClass;
	static Class<? extends Reducer<?, ?, ?, ?>> reduceClass;
	static boolean isCombiner;
	
	public static void init(){
		mapredJapPath = "E:\\AutoDataProcess\\zhanglb\\Expert_Fields_v3.jar";
		jobName = "Expert_Fields_v3_2";
		mapClass = Expert_Fields_v3_2.Expert_FieldsMaper.class;
		reduceClass = Expert_Fields_v3_2.Expert_FieldsReducer.class;
		combinerClass = null;
		isCombiner = false;
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
//		conf.setInt("mapred.reduce.tasks", 100);
		if (otherArgs.length < 2){
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		conf.set("mapred.jar",mapredJapPath);
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(ZhangMain.class);
		job.setMapperClass(mapClass);
		if(reduceClass != null){
			if(isCombiner){
				if(null == combinerClass){
					job.setCombinerClass(reduceClass);
				}else {
					job.setCombinerClass(combinerClass);
				}
			}
			job.setReducerClass(reduceClass);
		}
		
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
//		job.setOutputFormatClass(MyTextOutputFormat.class);
		
//		for(int i = 0;i < otherArgs.length-1;i++){
//			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
//		}
//		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		for(int i = 0;i<3;i++){
//			String path = "hdfs://10.1.13.111:8020/user/offlineCalculationData/zhang/fieldFindExpertsTmp/part-r-000" + String.format("%02d", i);
			String path = "hdfs://10.1.13.111:8020/user/offlineCalculationData/zhang/fieldFindExperts_" + i;
			FileInputFormat.addInputPath(job, new Path(path));
		}
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.1.13.111:8020/user/offlineCalculationData/zhang/fieldFindExperts"));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}
