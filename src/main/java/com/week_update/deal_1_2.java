package com.week_update;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: hqf
 * @description:
 * @Data: Create in 0:17 2018/8/20
 * @Modified By: 领域找人表去null
 */
public class deal_1_2 {
    public static class delete_nullmap extends Mapper<Object, Text, Text, NullWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String []items = line.split("\t");
            String outstr = "";
            if (items.length > 0){
                for(int i = 1 ; i < items.length ; ++i){
                    if (i == 1){
                        outstr += items[i];
                    }
                    else{
                        outstr = outstr + "\t" + items[i];
                    }
                }
            }
            context.write(new Text(outstr), NullWritable.get());
        }
    }
}
