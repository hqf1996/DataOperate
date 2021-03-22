package com.expert.addyuci;

import com.structure.ExpertPaperJoinStructure;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: yhj
 * @Description: 补充专家的领域云图，首先筛选出专家的成果（FilterExpertJoin），
 *               再关联分词(JoinCut)，再进行分词在领域云词中筛选(FilterWords)
 *               最后整合同个专家下所有的云词，统计词频
 * @Date: Created in 2018/7/5.
 */
public class FilterExpertJoin extends Configured implements Tool{

    public static class FilterExpertJoinMap extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            String path = ((FileSplit)inputSplit).getPath().toString();
            if(path.contains("join")){
                String[] infos = value.toString().split("\t");
                outKey.set(infos[ExpertPaperJoinStructure.EXPERT_ID]);
                outValue.set(infos[ExpertPaperJoinStructure.PAPER_ID]);
                context.write(outKey, outValue);
            }else {
                outValue.set("1");
                context.write(value, outValue);
            }
        }
    }

    public static class FilterExpertJoinReduce extends Reducer<Text, Text, Text, NullWritable>{
        private Text outKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean flag = false;
            List<String> achieveIds = new ArrayList<>();
            for(Text value: values){
                String str = value.toString();
                if(str.equals("1")){
                    flag = true;
                }else {
                    if(str != null && !str.equals("null") && !str.equals(""))
                        achieveIds.add(str);
                }
            }
            if(!achieveIds.isEmpty() && flag){
                for(String value: achieveIds){
                    outKey.set(key.toString() + "\t" + value);
                    context.write(outKey, NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
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
        conf.set("mapred.jar", "E:\\JavaProjects\\DataOperate\\target\\DataOperate.jar");
        Job job = Job.getInstance(conf);
        job.setJobName("file no yuci expert");
        job.setJarByClass(this.getClass());
        job.setMapperClass(FilterExpertJoinMap.class);
        job.setReducerClass(FilterExpertJoinReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 1: 0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new FilterExpertJoin(), args));
    }
}
