package com.zhanglb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * step2.2
 * 对输入数据按照领域下成果数前54个专家 筛选
 * 
 * 该类用于计算    专家ID	领域1 论文数 专利数 项目数 该领域下总数...     表中提取信息
 * 输出为 
 * 专家ID	领域1 论文数 专利数 项目数 该领域下总数
 * 专家ID	领域2 论文数 专利数 项目数 该领域下总数
 * 专家ID	领域3 论文数 专利数 项目数 该领域下总数
 *  格式
 *  做筛选  只取领域下成果数前54个专家
 *  
 * */
public class Expert_Fields_v3_2 {
	
	final static int RESULT_COUNT=54;//每个领域最多的专家的数量
	//final static int RESULT_RANGE_MIN=1;//返回结果中 机构评分的范围
	//final static int RESULT_RANGE_MAX=100;
	
	public static class Expert_FieldsMaper extends
			Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				String line = value.toString();
				String [] items = line.split("	");
				String EID = items[0];
				String [] col = items[1].split(" ");
				String field = col[0];
				context.write(new Text(field), value);
		}
	}
	
		public static class Expert_FieldsReducer 
				extends Reducer<Text, Text, Text, Text>{

			@Override
			protected void reduce(Text key, Iterable<Text> values,
					Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				Map<String, Integer> fieldExpertCountMap= new HashMap<String, Integer>();
				Map<String, String> fieldExpertResultMap= new HashMap<String, String>();
				for (Text each : values) {
					String value = each.toString();
					String [] items = value.split("	");
					String EID = items[0];
					String [] col = value.split(" ");
					Integer count  = Integer.valueOf(col[col.length-1]);
					fieldExpertCountMap.put(EID, count);
					fieldExpertResultMap.put(EID, value);
				}
				HeapSort heap =new HeapSort();
//				fieldExpertCountMap = heap.sortMap(fieldExpertCountMap, RESULT_COUNT);  // 取前RESULT_COUNT个的排序结果
				fieldExpertCountMap = heap.sortMap(fieldExpertCountMap);                // 取全部的排序结果
				Iterator<String> iterExpertFieldCountMap = fieldExpertCountMap.keySet().iterator(); 
				while(iterExpertFieldCountMap.hasNext()) {
					String EID = iterExpertFieldCountMap.next(); 
		    		String result = fieldExpertResultMap.get(EID);
		    		String [] items = result.split("	");
		    		if(items.length<2)
		    			return;
		    		context.write(new Text(items[0]), new Text(items[1]));
				}
			}
		}

		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Expert Fields");
			job.setJarByClass(Expert_Fields_v3_2.class);
			job.setMapperClass(Expert_FieldsMaper.class);
			job.setReducerClass(Expert_FieldsReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path("hdfs://10.1.13.111:8020/user/wuyc/zhanglb/PaperJoinWord/output/part-r-00000"));
			org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPaths(job, "hdfs://10.1.13.111:8020/user/zhanglb/fieldFindExperts/input/paper"
					+ ",hdfs://10.1.13.111:8020/user/zhanglb/fieldFindExperts/input/patent"//专利和论文的结果
					+ ",hdfs://10.1.13.111:8020/user/zhanglb/fieldFindExperts/input/project");
			FileOutputFormat.setOutputPath(job, new Path("hdfs://10.1.13.111:8020/user/zhanglb/fieldFindExperts/output"));
			System.exit(job.waitForCompletion(true)?0:1);
			
		}
			
	}
