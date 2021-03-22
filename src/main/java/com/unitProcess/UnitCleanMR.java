package com.unitProcess;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.structure.UniversityShortCallStructure;
import com.util.Util;

public class UnitCleanMR extends Configured implements Tool{
	public static class UnitCleanMap extends Mapper<Object, Text, Text, Text>{
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
		/*UnitCleaner unitCleaner;
		UniversitySim universitySim;
		InstitueSim institueSim;
		CompanySim companySim;
		HospitalSim hospitalSim;
		GovernmentSim governmentSim;*/
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
			/*universitySim = new UniversitySim(unitList, cityList);
			institueSim = new InstitueSim(unitList, cityList);
			companySim = new CompanySim(unitList, cityList);
			hospitalSim = new HospitalSim(unitList, cityList);
			governmentSim = new GovernmentSim(unitList, cityList);
			unitCleaner = new UnitCleaner(unitList);*/
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String unit = value.toString();
			/*unit = UnitCleaner.pretreatmentData(unit);
			UnitInfo unitInfo = unitCleaner.clean(unit);
			if(unitInfo != null && !unitInfo.getUnit().equals("null")){
				unitInfo.setRankCode(unitInfo.getTypeCode() + "1");
			}else{
				unitInfo = unitClassify(unit);
			}*/
			UnitInfo unitInfo = unitProcess.dispose(unit);
			outKey.set(unit + "\t" + unitInfo.join("\t"));
			outValue.set(unitInfo.getRankCode());
			context.write(outKey, outValue);
		}
		
		/*
		 * 判断字符串是否全是中文
		 */
		/*private boolean isChinese(String s){
			if(s == null || s.equals("")){
				return false;
			}
			Pattern pattern = Pattern.compile("[^\\u4E00-\\u9FBF]+");
			return !(pattern.matcher(s).find());
		}*/
		
		/**
		 * 为匹配上的单位分类
		 * @param unit
		 * @return
		 */
		/*private UnitInfo unitClassify(String unit){
			Pattern schoolPattern = Pattern.compile(".*?(学校|学院|大学)");
			Pattern instituePattern = Pattern.compile(".*?(研究所|研究院|科学院|科院|研发中心|研究中心|实验室|设计院|科研中心)");
			Pattern companyPattern = Pattern.compile(".*?(企业|公司|集团|厂)");
			Pattern hospitalPattern = Pattern.compile(".*?医院");
			Matcher matcher;
			UnitInfo unitInfo;
			if((matcher = schoolPattern.matcher(unit)).find()){  //学校的处理
				if(matcher.group(0).contains("科学院")){
					unitInfo = FailUnitClean.institueFailClean(unit, institueSim);
				}else{
					unitInfo = FailUnitClean.universityFailClean(unit, shortCallList, universitySim);
				}
			}
			else if ((matcher = instituePattern.matcher(unit)).find()){  //研究所的处理
				unitInfo = FailUnitClean.institueFailClean(unit, institueSim);
			}
			else if ((matcher = companyPattern.matcher(unit)).find()) {  //企业的处理
				unitInfo = FailUnitClean.companyFailClean(unit, companySim);
			}else if ((matcher = hospitalPattern.matcher(unit)).find()) { //医院的处理
				unitInfo = FailUnitClean.hospitalFailClean(unit, hospitalSim);
			}else {  //党政机关的处理
				unitInfo = FailUnitClean.governmentFailClean(unit, governmentSim);
			}
			if(unitInfo.getUnit().equals("null")){
				unit = unit.split("！|!|;|/|、|,| ")[0];
				if(isChinese(unit) && unit.length() > 4){
					unitInfo.setUnit(unit);
				}
			}
			return unitInfo;
		}*/
	}

	public static class UnitCleanReduce extends Reducer<Text, Text, Text, NullWritable>{
		MultipleOutputs<Text, NullWritable> outputs;
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			outputs = new MultipleOutputs<Text, NullWritable>(context);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(key, NullWritable.get());
			/*for(Text value:values){
				String info = value.toString();
				if(info.charAt(2) == '1'){
					outputs.write("cleanSuccess", key, NullWritable.get(), "cleanSuccess");
				}
				else if (info.equals(Message.COMMONUNIVERSITY)) {
					outputs.write("cleanFail", key, NullWritable.get(), "common_university");
				}else if (info.equals(Message.COMMONINSTITUES)) {
					outputs.write("cleanFail", key, NullWritable.get(), "common_institues");
				}else if (info.equals(Message.COMMONCOMPANY)) {
					outputs.write("cleanFail", key, NullWritable.get(), "common_company");
				}else if (info.equals(Message.COMMONHOSPITAL)) {
					outputs.write("cleanFail", key, NullWritable.get(), "common_hospital");
				}else if (info.equals(Message.COMMONGOVERNMENT)) {
					outputs.write("cleanFail", key, NullWritable.get(), "common_goverment");
				}else {
					outputs.write("cleanFail", key, NullWritable.get(), "other");
				}
			}*/
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			outputs.close();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String fs = "hdfs://10.1.13.111:8020";
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "unit clean");
		job.addCacheFile(new URI(fs + "/user/mysqlOut/unit"));
		job.addCacheFile(new URI(fs + "/user/mysqlOut/unit_citycode_map"));
		job.addCacheFile(new URI(fs + "/user/mysqlOut/university_short_call"));
		job.setJarByClass(UnitCleanMR.class);
		job.setMapperClass(UnitCleanMap.class);
		job.setReducerClass(UnitCleanReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "cleanSuccess", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "cleanFail", TextOutputFormat.class, Text.class, NullWritable.class);
		FileInputFormat.setMaxInputSplitSize(job, 4*1024*1024);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		String mapredJapPath = "E:\\AutoDataProcess\\unitTest\\UnitCleanMR.jar";
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
		System.exit(ToolRunner.run(conf, new UnitCleanMR(), otherArgs));
	}
}
