package com.expert.extract;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CrawlerPaperExtractExpertMain {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration(); // 启用默认配置
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		if (otherArgs.length != 2){
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		conf.set("mapred.jar","D:\\DataOperateJar\\CrawlerExtractExpert.jar");
//		conf.set("mapred.textoutputformat.separator", "");//自定义 key value之间的分隔符
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, " extract expert ");
		job.setJarByClass(CrawlerPaperExtractExpertMain.class);
		job.setMapperClass(CrawlerPaperExtractExpertMap.class);
//		job.setCombinerClass(Reduce.class);
//		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
