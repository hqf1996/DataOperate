package com.investment;

import com.structure.ProjectStructure;
import com.util.JobDefaultInit;
import com.util.Util;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/22.
 */
public class ExportProject extends Configured implements Tool {

    public static class ExportProjectMap extends Mapper<Object, Text, Text, NullWritable>{
        List<String[]> classCode;
        Map<String, String> boatNoMap;
        Text outKey = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            classCode = Util.getListFromDir("classCodeMap.txt", "\t", 4);
            boatNoMap = Util.getMapFromDir("project_class.txt", " ", 0, 1);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t");
            if(boatNoMap.containsKey(infos[ProjectStructure.PROJECT_ID])){
                String subject = boatNoMap.get(infos[ProjectStructure.PROJECT_ID]);
                infos[ProjectStructure.subject_code] = "其他";
                infos[ProjectStructure.subject_code_f] = subject;
                infos[ProjectStructure.area_code] = infos[ProjectStructure.province_code_f];
                for(String[] each: classCode){
                    if(subject.equals(each[2])){
                        infos[ProjectStructure.subject_code] = each[3];
                        break;
                    }
                }
                outKey.set(StringUtils.join("\t", infos));
                context.write(outKey, NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar", args);
        job.setJobName("Export investmemnt Project");
        job.addCacheFile(new URI(ExportPaper.fs + "/user/investment/classCodeMap.txt"));
        job.addCacheFile(new URI(ExportPaper.fs + "/user/investment/project_class.txt"));
        job.setMapperClass(ExportProjectMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true)? 1:0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new ExportProject(), args));
    }
}
