package com.week_update;

import com.util.Util;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 21:21 2018/8/17
 * @Modified By:
 */
public class AddNewExpert_Danwei {
    public static class addNewExpertDanweiMaper extends Mapper<Object, Text, Text, Text> {

        private Text keyText = new Text();
        private Text valueText = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String []items = line.split("\t");
            String outkey;
            if (items.length > 0) {
                //读专家数据和读新产生的数据
                keyText.set(items[0]); //id
                valueText.set(value.toString());
                //System.err.println(valueText);
                context.write(keyText, valueText);
//
//                outkey = items[0]; //id
//                context.write(new Text(outkey), new Text(line));
            }
        }
    }

    public static class addNewExpertDanweiReducer extends Reducer<Text, Text, Text, NullWritable> {
        private Text keyText = new Text();
        String[] dataList = new String[9999];

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            int number1 = 0;
            int number2 = 0;
            for (Text value : values) {
                dataList[i++] = value.toString();
                System.err.println(value);
            }
            System.err.println("数组长度" + i);
            System.err.println("\n");
            for (int l = 0 ; l < i ; ++l){
                if ((dataList[l].split("\t").length) == 2){ //专家表
                    number1++;
                }
                if ((dataList[l].split("\t").length) > 3){ //新表数据
                    number2++;
                }
            }
            //System.err.println("\n");
            if(number1 == 0 && number2 > 0){ //数据为新增
                for (int k = 0 ; k < i ; ++k) {
                    context.write(new Text(dataList[k]), NullWritable.get());
                }
            }
        }
    }
}
