package com.investment;

import com.structure.PatentStructure;
import com.util.JobDefaultInit;
import com.util.Util;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/22.
 */
public class UpdatePatent extends Configured implements Tool {
    public static class UpdatePatentMap extends Mapper<Object, Text, Text, NullWritable> {
        Map<String, String> updateNoMap;
        List<String[]> classCode;
        Text outKey = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            classCode = Util.getListFromDir("classCodeMap.txt", "\t", 4);
            updateNoMap = Util.getMapFromDir("patent_class.txt", " ", 0, 1);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t");
            if(updateNoMap.containsKey(infos[PatentStructure.PATENT_ID])){
                String subject = updateNoMap.get(infos[PatentStructure.PATENT_ID]);
                infos[PatentStructure.subject_code] = "其他";
                infos[PatentStructure.subject_code_f] = subject;
                for(String[] each: classCode){
                    if(subject.equals(each[2])){
                        infos[PatentStructure.subject_code] = each[3];
                        break;
                    }
                }
            }
            outKey.set(StringUtils.join("\t", infos));
            context.write(outKey, NullWritable.get());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar", args);
        job.setJobName("Update investmemnt Patent");
        job.addCacheFile(new URI(ExportPaper.fs + "/user/investment/classCodeMap.txt"));
        job.addCacheFile(new URI(ExportPaper.fs + "/user/investment/patent_class.txt"));
        job.setMapperClass(UpdatePatentMap.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true)? 1:0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new UpdatePatent(), args));
    }
}
