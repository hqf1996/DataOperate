package com.expert.addyuci;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author: yhj
 * @Description: 3、分词在领域云词中筛选
 * @Date: Created in 2018/7/5.
 */
public class FilterWords {

    public static class FilterWordsMap extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String path = ((FileSplit)context.getInputSplit()).getPath().toString();
            if(path.contains("fieldWords.txt")){
                outValue.set("1");
                context.write(value, outValue);
            }else {
                String[] infos = value.toString().split("\t",2);
                outValue.set(infos[0]);
                for(String word: infos[1].split(" ")){
                    outKey.set(word);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class FilterWordsReduce extends Reducer<Text, Text, Text, NullWritable>{
        private Text outKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean flag = false; //判断是否在知网的云词中
            Set<String> expertIds = new HashSet<>();
            for(Text value: values){
                String str = value.toString();
                if(str.equals("1")){
                    flag = true;
                }else {
                    expertIds.add(str);
                }
            }
            if(!expertIds.isEmpty() && flag){
                for(String expertId: expertIds){
                    outKey.set(expertId + "\t" + key.toString());
                    context.write(outKey, NullWritable.get());
                }
            }
        }
    }
}
