package com.week_update;

import com.util.HdfsUtil;
import com.util.JobFactor;
import com.util.Util;
import com.zhanglb.*;
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

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 10:29 2018/8/17
 * @Modified By:
 */
public class mymain {
    public static Logger LOG = LoggerFactory.getLogger(FieldFindExpertProcessJobcontrol.class);
    public static String fs = "hdfs://10.1.13.111:8020";

    private static Map<String, String[]> pathInit(Configuration conf, String[] inputPath){
        String currentDate = Util.getLocalData();
        Map<String, String[]> pathMap = new HashMap<String, String[]>();
        String pathHead = fs + "/user/test/hqftest/out" + currentDate;
//        String pathHead = fs + "/user/offlineCalculationData/zhang2018-08-21";
        HdfsUtil.createDir(conf, pathHead, LOG);


//        String[] step_add_expert = new String[101];
//        for(int i = 1;i<step_add_expert.length-1;i++){
//            step_add_expert[i] = "/user/offlineCalculationData/zhang2018-08-16/fieldFindExpertsTmp/part-r-000" + String.format("%02d", i);
//        }
//
//        String[] out_add_expert = new String[101];
//        for (int j = 0 ; j < out_add_expert.length-1 ; j++){
//            out_add_expert[j] = pathHead + "/add_expert/add_expert_" + String.format("%02d", j);
//        }
//
//        String[] name = new String[101];
//        for (int p = 1 ; p < name.length-1 ; p++){
//            name[p] = "test" + String.format("%02d", p);
//        }

        pathMap.put("test", new String[]{fs + "/user/test/hqftest/danwei.txt", fs + "/user/test/hqftest/sql_date.txt", pathHead + "/add_expert"});
//        for (int k = 1 ; k < 99 ; ++k){
//            pathMap.put(name[k], new String[]{out_add_expert[k-1], fs + step_add_expert[k], out_add_expert[k]});
//        }
        return pathMap;
    }

    private static JobControl createControlledJob(List<Job> jobsList) throws IOException {
        ControlledJob[] controlledJobs = new ControlledJob[jobsList.size()];
        for(int i=0;i<controlledJobs.length;i++){
            controlledJobs[i] = new ControlledJob(jobsList.get(i).getConfiguration());
            controlledJobs[i].setJob(jobsList.get(i));
            if(i != 0)
                controlledJobs[i].addDependingJob(controlledJobs[i - 1]);
        }
        JobControl jc = new JobControl("add week expert");
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
        jobTmp = JobFactor.createJob("test expert", conf, mymain.class, AddNewExpert_Fields.addNewExpertMaper.class,
                AddNewExpert_Fields.addNewExpertReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("test"));
        jobsList.add(jobTmp);

//        String[] name = new String[101];
//        for (int p = 1 ; p < name.length-1 ; p++){
//            name[p] = "test" + String.format("%02d", p);
//        }
//        for (int y = 1 ; y < 99 ; y++) {
//            jobTmp = JobFactor.createJob("test expert" + String.format("%02d", y), conf, mymain.class, AddNewExpert_Fields.addNewExpertMaper.class,
//                    AddNewExpert_Fields.addNewExpertReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get(name[y]));
//            jobsList.add(jobTmp);
//        }
        jobTmp = JobFactor.createJob("test expert", conf, mymain.class, AddNewExpert_Danwei.addNewExpertDanweiMaper.class,
                    AddNewExpert_Danwei.addNewExpertDanweiReducer.class, Text.class, Text.class, Text.class, NullWritable.class, pathMap.get("test"));
        jobsList.add(jobTmp);

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
