package com.week_update;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldFindExpertProcessJobcontrol {
	public static Logger LOG = LoggerFactory.getLogger(FieldFindExpertProcessJobcontrol.class);
	public static String fs = "hdfs://10.1.13.111:8020"; //hdfs://10.1.18.221:8020   psd:hdu52335335
	
	private static Map<String, String[]> pathInit(Configuration conf,String[] inputPath){
		String currentDate = Util.getLocalData();
		Map<String, String[]> pathMap = new HashMap<String, String[]>();
		String pathHead = fs + "/user/offlineCalculationData/zhang" + currentDate;
		HdfsUtil.createDir(conf, pathHead, LOG);
		HdfsUtil.createDir(conf, pathHead + "/preData", LOG);
		HdfsUtil.createDir(conf, pathHead + "/count", LOG);
		if(!(inputPath.length >= 3 && inputPath[0].contains("paper") && inputPath[1].contains("patent") && inputPath[2].contains("project"))){
			System.out.println("error:输入路径不正确！");
			System.exit(0);
		}
		pathMap.put("preCutPaper", new String[]{fs + "/user/offlineCalculationData/yuan/cut/paper/paper_cut2018-08-16.cut", fs + "/user/offlineCalculationData/yuan/cut_add_merge_words" + currentDate +"/paper"});
		pathMap.put("preCutPatent", new String[]{fs + "/user/offlineCalculationData/yuan/cut/patent/patent_cut2018-08-16.cut", fs + "/user/offlineCalculationData/yuan/cut_add_merge_words"+ currentDate +"/patent"});
		pathMap.put("preCutProject", new String[]{fs + "/user/offlineCalculationData/yuan/cut/project", fs + "/user/offlineCalculationData/yuan/cut_add_merge_words"+ currentDate +"/project"});
		
		pathMap.put("step1Paper", new String[]{inputPath[0],fs + "/user/offlineCalculationData/yuan/cut_add_merge_words"+ currentDate +"/paper" ,pathHead + "/preData/paper"});
		pathMap.put("step1Patent", new String[]{inputPath[1],fs + "/user/offlineCalculationData/yuan/cut_add_merge_words"+ currentDate +"/patent" , pathHead + "/preData/patent"});
		pathMap.put("step1Project", new String[]{inputPath[2],fs + "/user/offlineCalculationData/yuan/cut_add_merge_words"+ currentDate +"/project", pathHead + "/preData/project"});
		pathMap.put("step2.1", new String[]{pathHead + "/preData/paper", pathHead + "/preData/patent", pathHead + "/preData/project", pathHead + "/fieldFindExpertsTmp"});
		
		String[] step221Out = new String[33+1];
		for(int i = 0;i<step221Out.length-1;i++){
			step221Out[i] = pathHead + "/fieldFindExpertsTmp/part-r-000" + String.format("%02d", i); 
		}
		step221Out[step221Out.length-1] = pathHead + "/fieldFindExperts_0";
		pathMap.put("step2.2.1", step221Out);
		
		String[] step222Out = new String[33+1];
		int pos = step221Out.length -1;
		for(int i = 0;i<step222Out.length-1;i++){
			step222Out[i] = pathHead + "/fieldFindExpertsTmp/part-r-000" + String.format("%02d", pos + i); 
		}
		step222Out[step222Out.length-1] = pathHead + "/fieldFindExperts_1";
		pathMap.put("step2.2.2", step222Out);
		
		String[] step223Out = new String[33+1];
		pos = step221Out.length -1 + step222Out.length -1;
		for(int i = 0;i<step223Out.length-1;i++){
			step223Out[i] = pathHead + "/fieldFindExpertsTmp/part-r-000" + String.format("%02d", pos + i); 
		}
		step223Out[step223Out.length-1] = pathHead + "/fieldFindExperts_2";
		pathMap.put("step2.2.3", step223Out);
		
		pathMap.put("step2.2.4", new String[]{pathHead + "/fieldFindExperts_0", pathHead + "/fieldFindExperts_1", pathHead + "/fieldFindExperts_2", pathHead + "/fieldFindExperts"});

		pathMap.put("step3Paper", new String[]{inputPath[0], pathHead + "/count/paper"});
		pathMap.put("step3Patent", new String[]{inputPath[1], pathHead + "/count/patent"});
		pathMap.put("step3Project", new String[]{inputPath[2], pathHead + "/count/project"});
		pathMap.put("step4", new String[]{pathHead + "/count/paper", pathHead + "/count/patent", pathHead + "/count/project", pathHead + "/count/allTmp"});
		pathMap.put("step5", new String[]{pathHead + "/count/allTmp",fs + "/user/test/unitMap" , pathHead + "/count/all"});

		/**
		 * @Author: hqf
		 * @Date:
		 * @Description: 新增每周任务更新 单位找人表更新
		 */
		//pathMap.put("step_week_update1_1", new String[]{pathHead + "/count/all", fs + "/user/offlineCalculationData/zhang2018-08-16/fieldFindExpertsTmp", pathHead + "/week_update_DanweiFindExperts1"});
//		pathMap.put("step_week_update1_2", new String[]{pathHead + "/week_update_DanweiFindExperts1", fs + "/user/offlineCalculationData/zhang2018-08-16/fieldFindExperts_1/part-r-00000", pathHead + "/week_update_DanweiFindExperts2"});
//		pathMap.put("step_week_update1_3", new String[]{pathHead + "/week_update_DanweiFindExperts2", fs + "/user/offlineCalculationData/zhang2018-08-16/fieldFindExperts_2/part-r-00000", pathHead + "/week_update_DanweiFindExperts3"});


		pathMap.put("step6", new String[]{pathHead + "/count/all", pathHead + "/fieldFindExperts", pathHead + "/finalResult"});


		/**
		* @Author: hqf
		* @Date:
		* @Description: 新增每周任务更新 领域找人表更新
		*/
		//pathMap.put("step_week_update2_1", new String[]{pathHead + "/finalResult", fs + "/user/offlineCalculationData/zhang2018-08-16/fieldFindExpertsTmp", pathHead + "/week_update_FieldFindExperts1"});
//		pathMap.put("step_week_update2_2", new String[]{pathHead + "/week_update_FieldFindExperts1", fs + "/user/offlineCalculationData/zhang2018-08-16/fieldFindExperts_1/part-r-00000", pathHead + "/week_update_FieldFindExperts2"});
//		pathMap.put("step_week_update2_3", new String[]{pathHead + "/week_update_FieldFindExperts2", fs + "/user/offlineCalculationData/zhang2018-08-16/fieldFindExperts_2/part-r-00000", pathHead + "/week_update_FieldFindExperts3"});
		return pathMap;
	}
	
	private static JobControl createControlledJob(List<Job> jobsList) throws IOException{
		ControlledJob[] controlledJobs = new ControlledJob[jobsList.size()];
		for(int i=0;i<controlledJobs.length;i++){
			controlledJobs[i] = new ControlledJob(jobsList.get(i).getConfiguration());
			controlledJobs[i].setJob(jobsList.get(i));
			if(i != 0){
				controlledJobs[i].addDependingJob(controlledJobs[i-1]);
			}
		}
		JobControl jc = new JobControl("field find experts process");
		for(ControlledJob each:controlledJobs){
			jc.addJob(each);
		}
		return jc;
	}
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.addResource("core-site.xml");conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");conf.addResource("yarn-site.xml");
		List<Job> jobsList = new ArrayList<Job>();
		Map<String, String[]> pathMap = pathInit(conf, otherArgs);
		conf.set("mapreduce.job.jar","F:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar");
		Job jobTmp;
		jobTmp = JobFactor.createJob("pre cut paper", conf, FieldFindExpertProcessJobcontrol.class, PreCut.PreCutMap.class,
				Text.class, NullWritable.class, pathMap.get("preCutPaper"));
		jobTmp.addCacheArchive(new URI(fs + "/user/mysqlOut/merge_words"));
		jobsList.add(jobTmp);
		jobTmp = JobFactor.createJob("pre cut patent", conf, FieldFindExpertProcessJobcontrol.class, PreCut.PreCutMap.class,
				Text.class, NullWritable.class, pathMap.get("preCutPatent"));
		jobTmp.addCacheArchive(new URI(fs + "/user/mysqlOut/merge_words"));
		jobsList.add(jobTmp);
		jobTmp = JobFactor.createJob("pre cut project", conf, FieldFindExpertProcessJobcontrol.class, PreCut.PreCutMap.class,
				Text.class, NullWritable.class, pathMap.get("preCutProject"));
		jobTmp.addCacheArchive(new URI(fs + "/user/mysqlOut/merge_words"));
		jobsList.add(jobTmp);
		
		jobsList.add(JobFactor.createJob("pre export paper", conf, FieldFindExpertProcessJobcontrol.class, PreExportData.PreExportPaperDataMap.class,
				PreExportData.PreExportDataReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step1Paper")));	
		jobsList.add(JobFactor.createJob("pre export patent", conf, FieldFindExpertProcessJobcontrol.class, PreExportData.PreExportPatentDataMap.class,
				PreExportData.PreExportDataReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step1Patent")));	
		jobsList.add(JobFactor.createJob("pre export project", conf, FieldFindExpertProcessJobcontrol.class, PreExportData.PreExportProjectDataMap.class,
				PreExportData.PreExportDataReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step1Project")));
		
		Configuration conf3 = new Configuration();
		conf3.addResource("core-site.xml");conf3.addResource("hdfs-site.xml");
		conf3.addResource("mapred-site.xml");conf3.addResource("yarn-site.xml");
		conf3.set("mapreduce.job.jar","F:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar");
		conf3.setInt("mapreduce.job.reduces", 99);
		jobsList.add(JobFactor.createJob("expert field v3 1", conf3, FieldFindExpertProcessJobcontrol.class, Expert_Fields_v3_1.Expert_FieldsMaper.class,
				Expert_Fields_v3_1.Expert_FieldsReducer.class, Text.class, Text.class, Text.class, Text.class, pathMap.get("step2.1")));
		jobsList.add(JobFactor.createJob("expert field v3 2.1", conf, FieldFindExpertProcessJobcontrol.class, Expert_Fields_v3_2.Expert_FieldsMaper.class,
				Expert_Fields_v3_2.Expert_FieldsReducer.class, Text.class, Text.class, Text.class, Text.class, pathMap.get("step2.2.1")));
		jobsList.add(JobFactor.createJob("expert field v3 2.2", conf, FieldFindExpertProcessJobcontrol.class, Expert_Fields_v3_2.Expert_FieldsMaper.class,
				Expert_Fields_v3_2.Expert_FieldsReducer.class, Text.class, Text.class, Text.class, Text.class, pathMap.get("step2.2.2")));
		jobsList.add(JobFactor.createJob("expert field v3 2.3", conf, FieldFindExpertProcessJobcontrol.class, Expert_Fields_v3_2.Expert_FieldsMaper.class,
				Expert_Fields_v3_2.Expert_FieldsReducer.class, Text.class, Text.class, Text.class, Text.class, pathMap.get("step2.2.3")));
		jobsList.add(JobFactor.createJob("expert field v3 2.4", conf, FieldFindExpertProcessJobcontrol.class, Expert_Fields_v3_2.Expert_FieldsMaper.class,
				Expert_Fields_v3_2.Expert_FieldsReducer.class, Text.class, Text.class, Text.class, Text.class, pathMap.get("step2.2.4")));

		
		jobTmp = JobFactor.createJob("count expert paper", conf, FieldFindExpertProcessJobcontrol.class, CountExpertAchievement.CountExpertPaperMap.class,
				CountExpertAchievement.CountExpertPaperReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step3Paper"));
		jobTmp.addCacheFile(new URI(fs + "/user/mysqlOut/university_change_name"));
		jobsList.add(jobTmp);
		jobTmp = JobFactor.createJob("count expert patent", conf, FieldFindExpertProcessJobcontrol.class, CountExpertAchievement.CountExpertPatentMap.class,
				CountExpertAchievement.CountExpertPatentReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step3Patent"));
		jobTmp.addCacheFile(new URI(fs + "/user/mysqlOut/university_change_name"));
		jobsList.add(jobTmp);
		jobTmp = JobFactor.createJob("count expert project", conf, FieldFindExpertProcessJobcontrol.class, CountExpertAchievement.CountExpertProjectMap.class,
				CountExpertAchievement.CountExpertProjectReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step3Project"));
		jobTmp.addCacheFile(new URI(fs + "/user/mysqlOut/university_change_name"));
		jobsList.add(jobTmp);
		
		jobsList.add(JobFactor.createJob("count all expert achievement", conf, FieldFindExpertProcessJobcontrol.class, CountAllExpertAchievement.CountAllExpertAchievementMap.class,
				CountAllExpertAchievement.CountAllExpertAchievementReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step4")));
		jobTmp = JobFactor.createJob("count all expert achievement add province code", conf, FieldFindExpertProcessJobcontrol.class, CountAllExpertAchievementAddProvince.CountAllExpertAchievementAddProvinceMap.class,
				CountAllExpertAchievementAddProvince.CountAllExpertAchievementAddProvinceReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step5"));
		jobTmp.addCacheFile(new URI(fs + "/user/mysqlOut/unit"));
		jobTmp.addCacheFile(new URI(fs + "/user/mysqlOut/unit_citycode_map"));
		jobTmp.addCacheFile(new URI(fs + "/user/mysqlOut/university_short_call"));
		jobsList.add(jobTmp);

		/**
		 * @Author: hqf
		 * @Date:
		 * @Description: 每周专家更新
		 */
//		jobTmp = JobFactor.createJob("select new expert_1", conf, FieldFindExpertProcessJobcontrol.class, AddNewExpert_Danwei.addNewExpertDanweiMaper.class,
//				AddNewExpert_Danwei.addNewExpertDanweiReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step_week_update1_1"));
//		//暂时使用上次跑的数据
//		jobsList.add(jobTmp);

//		jobTmp = JobFactor.createJob("select new expert_2", conf, FieldFindExpertProcessJobcontrol.class, AddNewExpert_Danwei.addNewExpertDanweiMaper.class,
//				AddNewExpert_Danwei.addNewExpertDanweiReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step_week_update1_2"));
//		//暂时使用上次跑的数据
//		jobsList.add(jobTmp);
//
//		jobTmp = JobFactor.createJob("select new expert_3", conf, FieldFindExpertProcessJobcontrol.class, AddNewExpert_Danwei.addNewExpertDanweiMaper.class,
//				AddNewExpert_Danwei.addNewExpertDanweiReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step_week_update1_3"));
//		//暂时使用上次跑的数据
//		jobsList.add(jobTmp);


		Configuration conf4 = new Configuration();
		conf4.addResource("core-site.xml");conf4.addResource("hdfs-site.xml");
		conf4.addResource("mapred-site.xml");conf4.addResource("yarn-site.xml");
		conf4.set("mapreduce.job.jar","F:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar");
		conf4.setInt("mapreduce.job.reduces", 10);
		jobsList.add(JobFactor.createJob("final join", conf, FieldFindExpertProcessJobcontrol.class, FinalJoin.FinalJoinMap.class,
				FinalJoin.FinalJoinReduce.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step6")));


		/**
		 * @Author: hqf
		 * @Date:
		 * @Description: 每周专家更新
		 */
//		jobTmp = JobFactor.createJob("select new expert2_1", conf, FieldFindExpertProcessJobcontrol.class, AddNewExpert_Fields.addNewExpertMaper.class,
//				AddNewExpert_Fields.addNewExpertReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step_week_update2_1"));
//		//暂时使用上次跑的数据
//		jobsList.add(jobTmp);

//		jobTmp = JobFactor.createJob("select new expert2_2", conf, FieldFindExpertProcessJobcontrol.class, AddNewExpert_Fields.addNewExpertMaper.class,
//				AddNewExpert_Fields.addNewExpertReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step_week_update2_2"));
//		//暂时使用上次跑的数据
//		jobsList.add(jobTmp);
//
//		jobTmp = JobFactor.createJob("select new expert2_3", conf, FieldFindExpertProcessJobcontrol.class, AddNewExpert_Fields.addNewExpertMaper.class,
//				AddNewExpert_Fields.addNewExpertReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("step_week_update2_3"));
//		//暂时使用上次跑的数据
//		jobsList.add(jobTmp);

		JobControl jc = createControlledJob(jobsList);
		Thread jcThread = new Thread(jc);    
	    jcThread.start();    
	    while(true){    
	    	if(jc.allFinished()){    
	    		System.out.println(jc.getSuccessfulJobList());    
	    		jc.stop();    
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
