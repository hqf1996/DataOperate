package com.autoStep.project;

import com.util.HdfsUtil;
import com.util.JobFactor;
import com.util.Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class ProjectDataProcessJobControl {
	public static Logger LOG = LoggerFactory.getLogger(ProjectDataProcessJobControl.class);
	public static String fs = "hdfs://10.1.13.111:8020";
	public static boolean isUpdate = false;//判断是更新还是增加,默认为false
	public static String currentDate = Util.getLocalData();

	private static Map<String, String[]> pathInit(Configuration conf,String[] inputPath){
		Map<String, String[]> pathMap = new HashMap<String, String[]>();
		String pathHead = fs + "/user/mysqlAddData/project" + currentDate;
		HdfsUtil.createDir(conf, pathHead, LOG);
		//输入爬虫爬取的路径
		String[] prePath = new String[inputPath.length+1];
		for(int i = 0;i<inputPath.length;i++){
			prePath[i] = inputPath[i];
		}
		prePath[prePath.length-1] = pathHead + "/preProject";
		pathMap.put("preProject", prePath);
		// 设置每一个步骤要读取的位置和存放的位置，数组最后一个为存放的位置，之前的为要读取的位置
		pathMap.put("step1", new String[]{pathHead + "/preProject", fs + "/user/mysqlOut/project_all_expert", pathHead + "/step1"});
		pathMap.put("step2.0", new String[]{pathHead + "/step1", fs + "/user/mysqlOut/expert", pathHead + "/step2.0"});
		pathMap.put("step2.1", new String[]{pathHead + "/step2.0", fs + "/user/mysqlOut/expert", pathHead + "/step2.1"});
		pathMap.put("step2.2", new String[]{pathHead + "/step2.1", fs + "/user/mysqlOut/expert", pathHead + "/step2.2"});
		pathMap.put("step2.3", new String[]{pathHead + "/step2.2", fs + "/user/mysqlOut/expert", pathHead + "/step2"});
		pathMap.put("step3", new String[]{pathHead + "/step2", pathHead + "/step3"});
		pathMap.put("step4", new String[]{pathHead + "/step3", fs + "/user/mysqlOut/expert", pathHead + "/step4"});
		pathMap.put("step5", new String[]{pathHead + "/step4", pathHead + "/step2", pathHead + "/step5"});
		pathMap.put("step6", new String[]{pathHead + "/step5", pathHead + "/step6"});
		pathMap.put("step7", new String[]{pathHead + "/step4", pathHead + "/step7"});
		pathMap.put("step8", new String[]{pathHead + "/step4", fs + "/user/mysqlOut/expert", pathHead + "/step8"});
		pathMap.put("step9", new String[]{pathHead + "/step5", pathHead + "/step9"});
		pathMap.put("step10", new String[]{pathHead + "/step6", pathHead + "/step10"});
		return pathMap;
	}
	
	public static void resultMove(Configuration conf, Map<String, String[]> pathMap, boolean isUpdate){
		if(isUpdate){//更新时要先删除原来的文件
			HdfsUtil.removeFile(conf, HdfsUtil.listAll(conf, fs + "/user/mysqlOut/project_all_expert", LOG), LOG);
			HdfsUtil.removeFile(conf, HdfsUtil.listAll(conf, fs + "/user/mysqlOut/project", LOG), LOG);
			HdfsUtil.removeFile(conf, HdfsUtil.listAll(conf, fs + "/user/mysqlOut/expert_project_join", LOG), LOG);
		}
		HdfsUtil.mvFile(conf, pathMap.get("step9")[pathMap.get("step9").length-1] + "/part-r-00000", fs + "/user/mysqlOut/project_all_expert/add_project_all_expert" + currentDate, LOG);
		HdfsUtil.mvFile(conf, pathMap.get("step10")[pathMap.get("step10").length-1] + "/part-r-00000", fs + "/user/mysqlOut/project/add_project" + currentDate, LOG);
		HdfsUtil.mvFile(conf, pathMap.get("step7")[pathMap.get("step7").length-1] + "/part-r-00000", fs + "/user/mysqlOut/expert_project_join/add_expert_project_join" + currentDate, LOG);
		HdfsUtil.mvFile(conf, pathMap.get("step8")[pathMap.get("step8").length-1] + "/part-r-00000", fs + "/user/mysqlOut/expert/add_project_expert" + currentDate, LOG);
	} 
	
	public static void readIsUpdate(){
		System.out.println("please choose : update(1) or add(0)");
		Scanner scanner = new Scanner(System.in);
		while(true){
			int x = scanner.nextInt();
			if(x == 1){
				isUpdate = true;
				System.out.println("start update project data.....");
				break;
			}else if(x == 0){
				System.out.println("start add project data.....");
				break;
			}else {
				System.out.println("please input 1 or 0....");
			}
		}
		scanner.close();
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		//readIsUpdate();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.addResource("core-site.xml");conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");conf.addResource("yarn-site.xml");
		List<Job> jobList = new ArrayList<Job>();
		Map<String, String[]> pathMap = pathInit(conf, otherArgs);
		
		conf.set("mapreduce.job.jar","D:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar");
		
		//不同网页爬取的数据预处理方法不同
		Job preJob = JobFactor.createJob("preProject", conf, ProjectDataProcessJobControl.class, PretreatmentNstrs.Step1Map.class,
				PretreatmentNstrs.Step1Reduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("preProject"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/unit"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/unit_citycode_map"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/university_short_call"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/project_type_map"));
		//后来添加的缓存文件位置
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/projectArea.txt"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/projectJihua.txt"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/projectJihua2.txt"));
		jobList.add(preJob);

		/**
		if(isUpdate){
			jobList.add(JobFactor.createJob("Step1", conf, ProjectDataProcessJobControl.class, Step1Update.Step1Map.class, 
					Step1Update.Step1Reduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step1")));
		}else {
			jobList.add(JobFactor.createJob("Step1", conf, ProjectDataProcessJobControl.class, Step1Add.Step1Map.class, 
					Step1Add.Step1Reduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step1")));
		}
		 jobList.add(JobFactor.createJob("Step1", conf, ProjectDataProcessJobControl.class, Step1Add.Step1Map.class,
		 Step1Add.Step1Reduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step1")));
		 **/

		jobList.add(JobFactor.createJob("Step1", conf, ProjectDataProcessJobControl.class, Step1Add.Step1Map.class,
				Step1Add.Step1Reduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step1")));
		for(int i=0;i<4;i++){
			String jobName = "step2." + i;
			conf.setInt("author number", i);
			jobList.add(JobFactor.createJob(jobName, conf, ProjectDataProcessJobControl.class, Step2.Step2Map.class, Step2.Step2Reduce.class,
				Text.class, Text.class, Text.class, NullWritable.class, pathMap.get(jobName)));
		}

		Job step3Job = JobFactor.createJob("Step3", conf, ProjectDataProcessJobControl.class, Step3.Step3Map.class,
				Text.class, NullWritable.class, pathMap.get("step3"));
		step3Job.addCacheFile(new URI(fs + "/user/mysqlOut/university_change_name"));
		jobList.add(step3Job);
		
		jobList.add(JobFactor.createJob("Step4", conf, ProjectDataProcessJobControl.class, Step4.Step4Map.class,
				Step4.Step4Reduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step4")));
		jobList.add(JobFactor.createJob("Step5", conf, ProjectDataProcessJobControl.class, Step5.Step5Map.class,
				Step5.Step5Reduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step5")));
		jobList.add(JobFactor.createJob("Step6", conf, ProjectDataProcessJobControl.class, Step6.Step6Map.class,
				Text.class, NullWritable.class, pathMap.get("step6")));
		jobList.add(JobFactor.createJob("Step7", conf, ProjectDataProcessJobControl.class, Step7.Step7Map.class,
				Text.class, NullWritable.class, pathMap.get("step7")));
		jobList.add(JobFactor.createJob("Step8", conf, ProjectDataProcessJobControl.class, Step8.Step8Map.class,
				Step8.Step8Reduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step8")));
		jobList.add(JobFactor.createJob("Step9", conf, ProjectDataProcessJobControl.class, FilterEmoji.FilterEmojiMap.class,
				Text.class, NullWritable.class, pathMap.get("step9")));
		jobList.add(JobFactor.createJob("Step10", conf, ProjectDataProcessJobControl.class, FilterEmoji.FilterEmojiMap.class, Text.class, NullWritable.class, pathMap.get("step10")));
		ControlledJob[] controlledJobs = new ControlledJob[jobList.size()];
		for(int i = 0;i<jobList.size();i++){
			controlledJobs[i] = new ControlledJob(jobList.get(i).getConfiguration());
			controlledJobs[i].setJob(jobList.get(i));
		}
		
		for(int i = 1;i<controlledJobs.length;i++){
			controlledJobs[i].addDependingJob(controlledJobs[i-1]);
		}
		
		JobControl jc = new JobControl("project data process");
		for(ControlledJob each: controlledJobs){
			jc.addJob(each);
		}
		
		Thread jcThread = new Thread(jc);    
	    jcThread.start();    
	    while(true){    
	    	if(jc.allFinished()){    
	    		System.out.println(jc.getSuccessfulJobList());   
	    		jc.stop();    
				resultMove(conf,pathMap,isUpdate);
	            return;
	        }    
	        if(jc.getFailedJobList().size() > 0){    
	        	System.out.println(jc.getFailedJobList());    
	            jc.stop();    
	            return;    
	        }    
	    }
	}
}
