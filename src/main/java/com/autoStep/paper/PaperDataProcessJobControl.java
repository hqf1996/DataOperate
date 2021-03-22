package com.autoStep.paper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.util.HdfsUtil;
import com.util.JobFactor;
import com.util.Util;


/**
 * 目前更新到2018-3-23之后
 * @author yhj
 *
 */

public class PaperDataProcessJobControl {

	public static Logger LOG = LoggerFactory.getLogger(PaperDataProcessJobControl.class);
	public static String fs = "hdfs://10.1.13.101:8020";
	public static String currentDate = Util.getLocalData();
	
	public static Map<String, String[]> pathInit(Configuration conf, String[] inputPath){
		Map<String, String[]> pathMap = new HashMap<String, String[]>();
		String pathHead = fs + "/user/mysqlAddData/paper" + currentDate;
		HdfsUtil.createDir(conf, pathHead, LOG);
		String[] prePath = new String[inputPath.length+1];
		for(int i = 0;i<inputPath.length;i++){
			prePath[i] = inputPath[i];
		}
		prePath[prePath.length-1] = pathHead + "/prePaper"; 
		
		pathMap.put("prePaper", prePath);
		pathMap.put("step1.0", new String[]{pathHead + "/prePaper", fs + "/user/mysqlOut/expert", pathHead + "/step1.0"});
		pathMap.put("step1.1", new String[]{pathHead + "/step1.0", fs + "/user/mysqlOut/expert", pathHead + "/step1.1"});
		pathMap.put("step1.2", new String[]{pathHead + "/step1.1", fs + "/user/mysqlOut/expert", pathHead + "/step1.2"});
		pathMap.put("step1.3", new String[]{pathHead + "/step1.2", fs + "/user/mysqlOut/expert", pathHead + "/step1"});
		pathMap.put("step2", new String[]{pathHead + "/step1", pathHead + "/step2"});
		pathMap.put("step3", new String[]{pathHead + "/step2", fs + "/user/mysqlOut/expert", pathHead + "/step3"});
		pathMap.put("step4", new String[]{pathHead + "/step1", pathHead + "/step3", pathHead + "/step4"});
		pathMap.put("step5", new String[]{pathHead + "/step4", pathHead + "/step5"});
		pathMap.put("step6", new String[]{pathHead + "/step3", pathHead + "/step6"});
		pathMap.put("step7", new String[]{pathHead + "/step3", fs + "/user/mysqlOut/expert", pathHead + "/step7"});
		return pathMap;
	}

	public static void resultMove(Configuration conf, Map<String, String[]> pathMap){
		HdfsUtil.mvFile(conf, pathMap.get("step4")[pathMap.get("step4").length-1] + "/part-r-00000", fs + "/user/mysqlOut/paper_all_expert/add_Paper_all_expert" + currentDate, LOG);
		HdfsUtil.mvFile(conf, pathMap.get("step5")[pathMap.get("step5").length-1] + "/part-r-00000", fs + "/user/mysqlOut/paper/add_paper" + currentDate, LOG);
		HdfsUtil.mvFile(conf, pathMap.get("step6")[pathMap.get("step6").length-1] + "/part-r-00000", fs + "/user/mysqlOut/expert_paper_join/add_expert_paper_join" + currentDate, LOG);
		HdfsUtil.mvFile(conf, pathMap.get("step7")[pathMap.get("step7").length-1] + "/part-r-00000", fs + "/user/mysqlOut/expert/add_paper_expert" + currentDate, LOG);
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		conf.addResource("core-site.xml");conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");conf.addResource("yarn-site.xml");
		List<Job> jobList = new ArrayList<Job>();
		Map<String, String[]> pathMap = pathInit(conf, otherArgs);


		conf.set("mapred.jar","D:\\qq\\文档\\981188718\\FileRecv\\DataOperate\\DataOperate\\target\\DataOperate.jar");
		Job preJob = JobFactor.createJob("prePaper", conf, PaperDataProcessJobControl.class, PretreatmentZW.CleanMapper.class, 
				PretreatmentZW.CleanReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("prePaper"));
		//FileInputFormat.setInputPaths(preJob, new Path("hdfs://10.1.13.111:8020/user/addData/paper/add_paper_result2018-08-17.txt"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/unit"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/unit_citycode_map"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/university_short_call"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/subject_paper_map"));
		preJob.addCacheFile(new URI(fs + "/user/mysqlOut/journal_information"));

		jobList.add(preJob);
		
		for(int i=0;i<4;i++){
			String jobName = "step1." + i;
			conf.setInt("author number", i);
			jobList.add(JobFactor.createJob(jobName, conf, PaperDataProcessJobControl.class, Step1.AddAuthorUnitMap.class, Step1.AddAuthorUnitReduce.class,
				Text.class, Text.class, Text.class, NullWritable.class, pathMap.get(jobName)));
		}
		
		Job step2Job = JobFactor.createJob("step2", conf, PaperDataProcessJobControl.class, Step2.ExportAuthorsMap.class,
				Text.class, NullWritable.class, pathMap.get("step2"));
		step2Job.addCacheFile(new URI(fs + "/user/mysqlOut/university_change_name"));
		jobList.add(step2Job);
		
		jobList.add(JobFactor.createJob("step3", conf, PaperDataProcessJobControl.class, Step3.ExpertCreateUUIdMap.class, 
				Step3.ExpertCreateUUIdReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step3")));
		
		jobList.add(JobFactor.createJob("step4", conf, PaperDataProcessJobControl.class, Step4.PaperExpertJoinMap.class, 
				Step4.PaperExpertJoinReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step4")));
		
		jobList.add(JobFactor.createJob("step5", conf, PaperDataProcessJobControl.class, Step5.ExportAddPaperMap.class, 
				Text.class, NullWritable.class, pathMap.get("step5")));
		
		jobList.add(JobFactor.createJob("step6", conf, PaperDataProcessJobControl.class, Step6.ExportAddPaperExpertJoinMap.class, 
				Text.class, NullWritable.class, pathMap.get("step6")));
		
		jobList.add(JobFactor.createJob("Step7", conf, PaperDataProcessJobControl.class, Step7.ExportAddExpertMap.class, 
				Step7.ExportAddExpertReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step7")));
		
		ControlledJob[] controlledJobs = new ControlledJob[jobList.size()];
		for(int i = 0;i<controlledJobs.length;i++){
			controlledJobs[i] = new ControlledJob(jobList.get(i).getConfiguration());
			controlledJobs[i].setJob(jobList.get(i));
		}
		for(int i = 1;i<controlledJobs.length;i++){
			controlledJobs[i].addDependingJob(controlledJobs[i-1]);
		}
		
		JobControl jc = new JobControl("paper data process");
		for(ControlledJob each:controlledJobs){
			jc.addJob(each);
		}
		
		 Thread jcThread = new Thread(jc);    
	     jcThread.start();    
	     while(true){    
	    	 if(jc.allFinished()){    
	    		 System.out.println(jc.getSuccessfulJobList());    
	             jc.stop();
	             resultMove(conf, pathMap);
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
