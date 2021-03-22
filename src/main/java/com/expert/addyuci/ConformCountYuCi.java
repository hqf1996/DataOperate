package com.expert.addyuci;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @Author: yhj
 * @Description: 4、整合同个专家下所有的云词，统计词频
 * @Date: Created in 2018/7/5.
 */
public class ConformCountYuCi {
    public static class ConformCountYuCiMap extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outvalue = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t", 2);
            outKey.set(infos[0]);
            outvalue.set(infos[1]);
            context.write(outKey, outvalue);
        }
    }

    public static class ConformCountYuCiReduce extends Reducer<Text, Text, Text, NullWritable>{
        private Text outKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> wordCountMap = new HashMap<>();
            for(Text value: values){
                String str = value.toString();
                if(wordCountMap.containsKey(str)){
                    wordCountMap.put(str, wordCountMap.get(str) + 1);
                }else {
                    wordCountMap.put(str, 1);
                }
            }
            if(!wordCountMap.isEmpty()){
                List<Map.Entry<String, Integer>> sortWordCount = new ArrayList<>(wordCountMap.entrySet());
                Collections.sort(sortWordCount, new Comparator<Map.Entry<String, Integer>>() {
                    //按照值倒序
                    @Override
                    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                        return -(o1.getValue() - o2.getValue());
                    }
                });
                StringBuilder result = new StringBuilder(key.toString()+"\t");
                for(Map.Entry<String, Integer> item: sortWordCount){
                    result.append(item.getKey() + ",");
                }
                result.deleteCharAt(result.length()-1);
                outKey.set(result.toString());
                context.write(outKey, NullWritable.get());
            }
        }
    }
}
