package com.expert.addyuci;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/**
 * @Author: yhj
 * @Description: 2、关联分词
 * @Date: Created in 2018/7/5.
 */
public class JoinCut {

    public static class JoinCutMap extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String path = ((FileSplit)context.getInputSplit()).getPath().toString();
            if(path.contains("cut")){
                String[] infos = value.toString().split(" ", 2);
                outKey.set(infos[0]);
                Set<String> wordSet = new HashSet<>(Arrays.asList(infos[1].split(" ")));  //分词去重
                wordSet.remove("null");
                wordSet.remove("");
                outValue.set("0 " + StringUtils.join(wordSet, " "));
            }else {
                String[] infos = value.toString().split("\t");
                outKey.set(infos[1]);
                outValue.set("1 " + infos[0]);
            }
            context.write(outKey, outValue);
        }
    }

    public static class JoinCutReduce extends Reducer<Text, Text, Text, NullWritable>{
        private Text outKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> expertIds = new ArrayList<>();
            String words = null;
            for(Text value:values){
                String[] infos = value.toString().split(" ", 2);
                if(infos[0].equals("0")){ //分词
                    words = infos[1];
                }else { // 专家id
                    expertIds.add(infos[1]);
                }
            }
            if(words != null && !expertIds.isEmpty()){
                for(String expertId: expertIds){
                    outKey.set(expertId + "\t" + words);
                    context.write(outKey, NullWritable.get());
                }
            }
        }
    }
}
