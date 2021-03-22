package com.investment;

import com.structure.PaperStructure;
import com.util.JobDefaultInit;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: yhj
 * @Description: 提取出船舶论文关键字，保存合适格式
 * @Date: Created in 2018/7/19.
 */
public class ExportKeyWords extends Configured implements Tool {
    public static class ExportKeyWordsMap extends Mapper<Object, Text, Text, NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyWords = value.toString().split("\t")[PaperStructure.keywords].split(";|/|,");
            List<String> l = new ArrayList<>();
            for(String each: keyWords){
                StringBuilder result = new StringBuilder("[");
                if(!each.equals("") && !each.equals("null")){
                    l.add(each);
                }
            }
            if(!l.isEmpty()){
                context.write(new Text("['" + StringUtils.join("','", l) + "']"), NullWritable.get());
            }
         }
    }

    public static class ExportKeyWordsReduce extends Reducer<Text, NullWritable, Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar", args) ;
        job.setJobName("Export KeyWords");
        job.setMapperClass(ExportKeyWords.ExportKeyWordsMap.class);
        job.setReducerClass(ExportKeyWords.ExportKeyWordsReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true)? 1:0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new ExportKeyWords(), args));
    }
}
