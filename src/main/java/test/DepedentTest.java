package test;

import com.util.JobDefaultInit;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;

/**
 * @Author: yhj
 * @Description:
 * @Date: Created in 2018/7/11.
 */
public class DepedentTest extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobDefaultInit.getSubmintDefaultJob(this, getConf(),
                "E:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar", args);
        job.setMapperClass(DepedentTestMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJobName("DepedentTest");
        return job.waitForCompletion(true)? 0 : 1;

    }

    public static class DepedentTestMap extends Mapper<Object, Text, Text, NullWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Document document = Jsoup.parse("dafafa");
            Elements elements = document.getElementsByClass("zyao");
            context.write(value, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception{
        System.exit(ToolRunner.run(new DepedentTest(), args));
    }
}
