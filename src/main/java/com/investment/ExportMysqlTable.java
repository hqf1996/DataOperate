package com.investment;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;
import com.util.JobDefaultInit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/20.
 */
public class ExportMysqlTable extends Configured implements Tool {

    public static class ExportPaperMysqlTableMap extends Mapper<Object, Text, Text, Text>{
        Text outKey = new Text();
        Text outVlaue = new Text();
        int totalNum;
        int firstExpertIdIndex;
        int subjectCodeIndex;
        int unitCodeIndex;
        int unitTypeIndex;
        int provinceCodeIndex;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalNum = context.getConfiguration().getInt("totalNum", 0);
            firstExpertIdIndex = context.getConfiguration().getInt("firstExpertIdIndex", 0);
            subjectCodeIndex = context.getConfiguration().getInt("subjectCodeIndex", 0);
            unitCodeIndex = context.getConfiguration().getInt("unitCodeIndex", 0);
            unitTypeIndex = context.getConfiguration().getInt("unitTypeIndex", 0);
            provinceCodeIndex = context.getConfiguration().getInt("provinceCodeIndex", 0);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String path = ((FileSplit)context.getInputSplit()).getPath().toString();
            String[] infos = value.toString().split("\t");
            if(path.contains("zhang2018-05-29")){   //姓名找人表只需要前9个字段
                String[] newInfos = new String[9];
                for(int i= 0;i < 9; i++){
                    newInfos[i] = infos[i];
                }
                outKey.set(infos[0]);
                outVlaue.set(StringUtils.join("\t", newInfos));
                context.write(outKey, outVlaue);
            }else {
                String[] newInfos = new String[totalNum];
                for(int i = 0;i < totalNum; i++){
                    newInfos[i] = infos[i];
                }
                String head = StringUtils.join("\t", newInfos);  //输出的前缀，成果表的内容
                boolean isFirst = false; //判断是否是第一个提取出的专家
                for(int i = 0;i < 4;i ++){
                    if(!infos[firstExpertIdIndex + i].equals("null")){
                        outKey.set(infos[firstExpertIdIndex + i]);
                        outVlaue.set(head + "\t" + infos[subjectCodeIndex] + "\t" +
                                infos[unitCodeIndex]+ "\t" +
                                infos[unitTypeIndex] + "\t" +
                                infos[provinceCodeIndex] + "\t" + (isFirst == true? "0":"1"));
                        if(!isFirst){
                            isFirst = true;
                        }
                        context.write(outKey, outVlaue);
                    }
                }
            }
        }
    }

    public static class ExportPaperMysqlTableReduce extends Reducer<Text, Text, Text, NullWritable>{
        Text outKey = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> achievements = new ArrayList<>();
            String expertInfo = null;
            for(Text value:values){
                String line = value.toString();
                String[] infos = line.split("\t");
                if(infos.length == 9){ //姓名找人中的信息
                    expertInfo = line;
                }else {
                    achievements.add(line);
                }
            }
            if(!achievements.isEmpty() && expertInfo != null){
                for(String each: achievements){
                    outKey.set(each + "\t" + expertInfo);
                    context.write(outKey, NullWritable.get());
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.setInt("totalNum", ProjectStructure.totalNum);
        conf.setInt("firstExpertIdIndex", ProjectStructure.first_author_f);
        conf.setInt("subjectCodeIndex", ProjectStructure.subject_code_f);
        conf.setInt("unitCodeIndex", ProjectStructure.unit_code_f);
        conf.setInt("unitTypeIndex", ProjectStructure.unit_type);
        conf.setInt("provinceCodeIndex",  ProjectStructure.province_code_f);
        Job job = JobDefaultInit.getSubmintDefaultJob(this, conf,
                "E:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar", args);
        job.setJobName("Export Mysql Table");
        job.setMapperClass(ExportPaperMysqlTableMap.class);
        job.setReducerClass(ExportPaperMysqlTableReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true)? 1:0;
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new ExportMysqlTable(), args));
    }
}
