package com.week_update;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 0:35 2018/8/20
 * @Modified By:单位找人表加null
 */
public class deal_1_1 {
    public static class add_nullmap extends Mapper<Object, Text, Text, NullWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String []items = line.split("\t");
            String outstr = "null";
            if (items.length > 0){
                for(int i = 0 ; i < items.length ; ++i){
                        outstr = outstr + "\t" + items[i];
                }
            }
            context.write(new Text(outstr), NullWritable.get());
        }
    }
}
