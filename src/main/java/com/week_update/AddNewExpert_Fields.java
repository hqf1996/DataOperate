package com.week_update;

import com.util.Util;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * @Author: hqf
 * @description: 将上一次更新的Expert_Fields_v3.2排序完成的专家导入，与刚更新完成的专家表进行比对，只提取新增的专家信息，为了方便
 * 下次每周的（不准确的）更新，则将新更新的专家表加入到上一个Expert_Fields_v3.2专家集中。但每月更新时需要全部重算。
 *
 * 输入/user/offlineCalculationData/zhang2018-08-16/fieldFindExperts
 *
 * @Data: Create in 10:11 2018/8/17
 * @Modified By:
 */
public class AddNewExpert_Fields {
    public static class addNewExpertMaper extends Mapper<Object, Text, Text, Text>{

        private Text keyText = new Text();
        private Text keyText2 = new Text();
        private Text valueText = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String []items = line.split("\t");
            String outkey;
            if (items.length > 0) {
                //读专家数据和读新产生的数据
               if (!items[0].equals("null")) {  //专家表
                   keyText.set(items[0]); //id
                   valueText.set(value.toString());
                   System.err.println(keyText + ":" + valueText);
                   context.write(keyText, valueText);
               }
               else{ //读取到了领域找人表
                   keyText2.set(items[1]);
                   valueText.set(value.toString());
                   System.err.println(items[1] + ":" + valueText);
                   context.write(keyText2, valueText);
               }
            }

        }
    }

    public static class addNewExpertReducer extends Reducer<Text, Text, Text, NullWritable>{
        private Text keyText = new Text();
        String[] dataList = new String[20000];

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            int number1 = 0;
            int number2 = 0;
            for (Text value : values) {
                dataList[i++] = value.toString();
                System.err.println(value.toString());
            }
            System.err.println("数组长度为" + i);
            System.err.println("\n");
            for (int l = 0 ; l < i ; ++l){
                if (dataList[l].split("\t").length == 2){ //专家表
                    number1++;
                }
                if (dataList[l].split("\t").length > 3){ //新表数据
                    number2++;
                }
            }
            if(number1 == 0 && number2 > 0){ //数据为新增
                for (int k = 0 ; k < i ; ++k) {
                    context.write(new Text(dataList[k]), NullWritable.get());
                }
            }
        }
    }
}
