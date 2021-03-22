package com.updateTableUnit;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.structure.UniversityShortCallStructure;
import com.unitProcess.UnitInfo;
import com.unitProcess.UnitProcess;
import com.util.Util;

/**
 * step1 处理好单位
 * @author yhj
 *
 */
public class AchievementUnitUpdate extends Configured implements Tool{
	public static class PaperUnitUpdateMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
//		int disposeUnitP = PaperStructure.first_organization; //需要处理的单位的字段位置
//		int totalNum = PaperStructure.totalNum; //字段总数
//		String pathStr = "paper"; //成果的表所在路径包含的字段
		int disposeUnitP = PatentStructure.fourth_inventor_unit; 
//		int totalNum = ProjectStructure.totalNum-1; 
		String pathStr = "patent"; 
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String filepath = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(filepath.contains(pathStr)){
				outKey.set(infos[disposeUnitP]);
//				outValue.set(StringUtils.join(Arrays.copyOfRange(infos, 0, totalNum+1), "\t"));
				outValue.set(value.toString());
				context.write(outKey, outValue);
			}
			if(filepath.contains("unitMap")){ //单位对照表，格式固定
				outKey.set(infos[0]);
				outValue.set(StringUtils.join(Arrays.copyOfRange(infos, 1, infos.length), "\t"));
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class PaperUnitUpdateReduce extends Reducer<Text, Text, Text, NullWritable>{
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
//		int updateUnitP = PaperStructure.unit; //跟新单位的位置
//		int totalNum = PaperStructure.totalNum;	//字段总数
		int updateUnitP = PatentStructure.fourth_inventor_unit;
		int totalNum = PatentStructure.fourth_author_rank_code-1;
//		int unitTypeP = ProjectStructure.unit_type-1;
		Text outKey = new Text();
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String unit = key.toString();
			List<String[]> papers = new ArrayList<String[]>();
			String[] unitInfo = null;
			if(unit.equals("null") || unit.equals("")){ //单位为空
				for(Text value:values){
					String[] infos = value.toString().split("\t");
					if(infos.length == totalNum+1){  //如果是paper表的信息
						infos[updateUnitP] = "null";
//						infos[updateUnitP] = infos[updateUnitP] + "\t" + "null";
//						infos[unitTypeP] = "99";
//						outKey.set(StringUtils.join(infos, "\t") + "\t" + "null\t99\t99\t99999999\t999\tnull");
						outKey.set(StringUtils.join(infos, "\t") + "\t999");
						context.write(outKey, NullWritable.get());
					}
				}
				return;
			}
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == totalNum+1){  //区分来自paper表还是单位对应表
					papers.add(infos);
				}
				if(infos.length == 6){
					unitInfo = infos;
				}
			}
			if(unitInfo != null && !papers.isEmpty()){  //填入对应的单位信息
				for(String[] paper:papers){
					paper[updateUnitP] = unitInfo[0];
//					paper[updateUnitP] = paper[updateUnitP] + "\t" + unitInfo[0];
//					paper[unitTypeP] = unitInfo[1];
					String result = StringUtils.join(unitInfo, "\t");
//					outKey.set(StringUtils.join(paper, "\t") + "\t" + result);
					outKey.set(StringUtils.join(paper, "\t") + "\t" + unitInfo[4]);
					System.out.println("映射处理:" + paper[PaperStructure.id+1] + "\t" + unit + "\t" + result);
					context.write(outKey, NullWritable.get());
				}
			}else {
				if(!papers.isEmpty()){  //没有对应的单位,采用单位处理的方法进行
					UnitInfo unitInfo2 = unitProcess.dispose(unit);
					for(String[] paper:papers){
						paper[updateUnitP] = unitInfo2.getUnit();
//						paper[updateUnitP] = paper[updateUnitP] + "\t" + unitInfo2.getUnit();
//						paper[unitTypeP] = unitInfo2.getTypeCode();
						String result =  unitInfo2.join("\t");
//						outKey.set(StringUtils.join(paper, "\t") + "\t" + result);
						outKey.set(StringUtils.join(paper, "\t") + "\t" + unitInfo2.getRankCode());
						System.out.println("程序处理:" + paper[PaperStructure.id+1] + "\t" + unit + "\t" + result);
						context.write(outKey, NullWritable.get());
					}
				}
			}
		}
	}
	
	public static class PatentUnitUpdateMap extends Mapper<Object, Text, Text, NullWritable>{
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
		Text outKey = new Text();
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String fs = "hdfs://10.1.13.111:8020";
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "Achievement Unit Update Patent Fourth Author Unit");
		job.addCacheFile(new URI(fs + "/user/mysqlOut/unit"));
		job.addCacheFile(new URI(fs + "/user/mysqlOut/unit_citycode_map"));
		job.addCacheFile(new URI(fs + "/user/mysqlOut/university_short_call"));
		job.setJarByClass(AchievementUnitUpdate.class);
		job.setMapperClass(PaperUnitUpdateMap.class);
		job.setReducerClass(PaperUnitUpdateReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		String mapredJapPath = "E:\\AutoDataProcess\\updateTableUnit\\AchievementUnitUpdatePatentFourthAuthorUnit.jar";
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
		conf.setInt("mapred.reduce.tasks", 10); 
		conf.set("mapred.jar",mapredJapPath);
		System.exit(ToolRunner.run(conf, new AchievementUnitUpdate(), otherArgs));
	}
}
