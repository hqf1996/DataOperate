package com.investment;

import com.structure.PaperStructure;
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

/**
 * @Author: yhj
 * @Description: 提取出和船舶相关的论文，并填上标签（替换上subject_code_old，subject_code_old，subject_code_f三个字段）
 * @Date: Created in 2018/7/19.
 */
public class ExportPaper extends Configured implements Tool{
    public static final String fs = "hdfs://10.1.13.111:8020";
    private static final String cbCode = "C036";

    public static class ExportPaperMap extends Mapper<Object, Text, Text, NullWritable>{
        List<String[]> classCode;
        Text outKey = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            classCode = Util.getListFromDir("classCodeMap.txt", "\t", 4);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t");
            if(infos[PaperStructure.zw_subject_code].equals(cbCode)){ ///判断是否为船舶工业
                infos[PaperStructure.subject_code_old] = "其他";
                infos[PaperStructure.subject_code_old] = "其他";
                infos[PaperStructure.subject_code_f] = "J";
                for(String[] each: classCode){ //写入中图分类的中文
                    if(infos[PaperStructure.classification].contains(each[0])){
                        infos[PaperStructure.subject_code_old] = each[1];
                        infos[PaperStructure.subject_code] = each[3];
                        infos[PaperStructure.subject_code_f] = each[2];
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
        job.setJobName("Export investmemnt Paper");
        job.addCacheFile(new URI(fs + "/user/investment/classCodeMap.txt"));
        job.setMapperClass(ExportPaper.ExportPaperMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true)? 1:0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new ExportPaper(), args));
    }
}
