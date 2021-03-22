package com.investment;

import com.structure.PaperStructure;
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
public class UpdatePaper extends Configured implements Tool {
    public static class UpdatePaperMap extends Mapper<Object, Text, Text, NullWritable>{
        Map<String, String> updateNoMap;
        List<String[]> classCode;
        Text outKey = new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            classCode = Util.getListFromDir("classCodeMap.txt", "\t", 4);
            updateNoMap = Util.getMapFromDir("paper_class.txt", " ", 0, 1);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] infos = value.toString().split("\t");
            if(updateNoMap.containsKey(infos[PaperStructure.PAPER_ID])){
                String subject = updateNoMap.get(infos[PaperStructure.PAPER_ID]);
                infos[PaperStructure.subject_code] = "其他";
                infos[PaperStructure.subject_code_f] = subject;
                for(String[] each: classCode){
                    if(subject.equals(each[2])){
                        infos[PaperStructure.subject_code] = each[3];
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
        job.setJobName("Update investmemnt Paper");
        job.addCacheFile(new URI(ExportPaper.fs + "/user/investment/classCodeMap.txt"));
        job.addCacheFile(new URI(ExportPaper.fs + "/user/investment/paper_class.txt"));
        job.setMapperClass(UpdatePaperMap.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true)? 1:0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new UpdatePaper(), args));
    }
}
