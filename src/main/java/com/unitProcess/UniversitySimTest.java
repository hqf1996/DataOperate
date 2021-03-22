package com.unitProcess;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.util.Util;

public class UniversitySimTest extends Configured implements Tool{
	public static class UniversitySimTestMap extends Mapper<Object, Text, Text, NullWritable>{
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> university9850r211;
		Text outKey = new Text();
		MultipleOutputs<Text, NullWritable> outputs;
		UniversitySim universitySim;
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			university9850r211 = Util.getListFromDir("university_985or211", 1);
			outputs = new MultipleOutputs<>(context);
			universitySim = new UniversitySim(unitList, cityList);
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String unit = value.toString();
			String[] newUnitsInfos = universitySim.similarity(unit);
			if(Double.valueOf(newUnitsInfos[newUnitsInfos.length-1]) >= 0.86){//余弦相似度大于0.86
				outKey.set(value.toString() + "\t" + newUnitsInfos[0] + " " + newUnitsInfos[1]);
				outputs.write("universitySuccess", outKey, NullWritable.get(), "UniversitySuccess");
			}else {
				//从匹配到985 211高校的单位中找出余弦相似度top5,并计算最大公共子串长度
				for(String[] s : university9850r211){
					if(newUnitsInfos[0].contains(s[0])){
						List<String[]> list = UniversitySim.topSimilarity(unit, unitList, cityList, 5, new SimilarityFun() {
							
							@Override
							public double run(String source, String target) {
								// TODO Auto-generated method stub
								return StringSimilarity.cosineSimilarity(source, target);
							}
						}, new SimilarityFun() {
							
							@Override
							public double run(String source, String target) {
								// TODO Auto-generated method stub
								return StringSimilarity.LongestCommonSubsequence(source, target);
							}
						});
						String result = "";
						for(String[] ss:list){
							result = result + "\t" +StringUtils.join(ss, " ");
						}
						outKey.set(value.toString() + "\t" + result);
						outputs.write("universityFail985or211", outKey, NullWritable.get(), "universityFail985or211");
						break;
					}
				}
				outKey.set(value.toString() + "\t" + newUnitsInfos[0] + " " + newUnitsInfos[1]);
				outputs.write("universityFail", outKey, NullWritable.get(), "UniversityFail");
			}
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		String fs = "hdfs://10.1.13.111:8020";
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "University Sim Test");
		job.addCacheFile(new URI(fs + "/user/mysqlOut/unit"));
		job.addCacheFile(new URI(fs + "/user/mysqlOut/unit_citycode_map"));
		job.addCacheFile(new URI(fs + "/user/mysqlOut/university_985or211"));
		job.setJarByClass(UniversitySimTest.class);
		job.setMapperClass(UniversitySimTestMap.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "universitySuccess", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "universityFail", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "universityFail985or211", TextOutputFormat.class, Text.class, NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileInputFormat.addInputPath(job, new Path(arg0[1]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[2]));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		String mapredJapPath = "E:\\AutoDataProcess\\unitTest\\UniversitySimTest.jar";
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2){
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		conf.set("mapred.jar",mapredJapPath);
		conf.setInt("mapred.reduce.tasks", 1);
		System.exit(ToolRunner.run(conf, new UniversitySimTest(), otherArgs));
	}
}
