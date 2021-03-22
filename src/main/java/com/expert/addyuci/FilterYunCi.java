package com.expert.addyuci;

import com.util.JobDefaultInit;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/31.
 */
public class FilterYunCi extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\DataOperate\\target\\DataOperate.jar", args);
        job.setJobName("filter yunci");
        job.setJarByClass(this.getClass());
//        job.setNumReduceTasks(0);
        job.setMapperClass(FilterYunCiMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 1: 0;
    }

    public static class FilterYunCiMap extends Mapper<Object, Text, Text, NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t");
            String[] words = infos[1].split(",");
            if(words.length > 100){
                words = Arrays.copyOfRange(words, 0, 100);
            }
            context.write(new Text(infos[0] + "\t" + StringUtils.join(",", words)), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new FilterYunCi(), args));
    }
}
