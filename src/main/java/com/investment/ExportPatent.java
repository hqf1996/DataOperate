package com.investment;

import com.structure.PatentStructure;
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
 * @Date: Created in 2018/7/20.
 */
public class ExportPatent extends Configured implements Tool {

    public static class ExportPatentMap extends Mapper<Object, Text, Text, NullWritable>{
        List<String[]> classCode;
        Map<String, String> boatNoMap;
        Text outKey = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            classCode = Util.getListFromDir("classCodeMap.txt", "\t", 4);
            boatNoMap = Util.getMapFromDir("patent_boat.txt", "\t", 0, 1);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t");
            if(boatNoMap.containsKey(infos[PatentStructure.publication_no])){
                String subject_old = boatNoMap.get(infos[PatentStructure.publication_no]);
                infos[PatentStructure.subject_code_old] = subject_old;
                infos[PatentStructure.subject_code] = "其他";
                infos[PatentStructure.subject_code_f] = "J";
                for(String[] each: classCode){
                    if(subject_old.equals(each[1])){
                        infos[PatentStructure.subject_code] = each[3];
                        infos[PatentStructure.subject_code_f] = each[2];
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
        job.setJobName("Export investmemnt Patent");
        job.addCacheFile(new URI(ExportPaper.fs + "/user/investment/classCodeMap.txt"));
        job.addCacheFile(new URI(ExportPaper.fs + "/user/investment/patent_boat.txt"));
        job.setMapperClass(ExportPatentMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true)? 1:0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new ExportPatent(), args));
    }
}
