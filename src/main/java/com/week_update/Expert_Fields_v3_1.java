package com.week_update;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;


/**
 * step2.1
 * 以专家为粒度，输出多个reduce文件，保证任何一个专家ID只会出现在同一个reduce文件中
 * 
 * 该类用于计算    成果id（三个空格）专家1ID(一个空格)专家1flag（1个空格）专家2ID(一个空格)专家2flag...（1个空格）专家nID(一个空格)专家nflag（三个空格）分词1（一个空格）分词2（一个空格）分词3...     表中提取信息
 * 输出为 
 * 专家ID 领域1 论文数 专利数 项目数 该领域下总数
 * 专家ID 领域2 论文数 专利数 项目数 该领域下总数
 * 专家ID 领域3 论文数 专利数 项目数 该领域下总数
 *  格式
 *   
 *  
 * */
public class Expert_Fields_v3_1 {
	
	//final static int RESULT_COUNT=54;//每个领域最多的专家的数量
	//final static int RESULT_RANGE_MIN=1;//返回结果中 机构评分的范围
	//final static int RESULT_RANGE_MAX=100;
	
	public static class Expert_FieldsMaper extends
			Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value,
				Context context)
				throws IOException, InterruptedException {
				String filepath = ((FileSplit)context.getInputSplit()).getPath().toString();//获取当前操作的行从哪个文件来
				String type = new String();
				if(filepath.indexOf("patent")!=-1){
					type = "patent";
				}else if(filepath.indexOf("paper")!=-1){
					type = "paper";
				}else if(filepath.indexOf("project")!=-1){
					type = "project";
				}
				String line = value.toString();
				String [] items = line.split("   ");
				if(items.length<3) return;
				//String [] experts = items[1].split("  ");
				//临时，等输入文件格式正确后替换回上面一行
				String [] expertsTemp = items[1].split(" ");
				String [] experts = new String[expertsTemp.length/2];
				for (int i = 0; i < experts.length; i++) {
					experts[i] = expertsTemp[i*2];
				}
				//end
				String [] fields = items[2].split(" ");

				//由于分词存在相同的词，使用set去除相同的词
				Set<String> fieldSet = new HashSet(Arrays.asList(fields));
				fields = fieldSet.toArray(new String[fieldSet.size()]);
				//

				for (int i = 0; i < fields.length; i++) {
					for (int j = 0; j < experts.length; j++) {
						String [] expertInfo = experts[j].split(" ");
						String EID = expertInfo[0];
						//System.err.println(EID+" "+fields[i]+" "+type+" 1");
						context.write(new Text(EID), new Text(fields[i]+" "+type+" 1"));
					}
				}
		}
	}

		public static class Expert_FieldsCombiner
				extends Reducer<Text, Text, Text, Text>{

			@Override
			protected void reduce(Text key, Iterable<Text> values,
					Context context)
					throws IOException, InterruptedException {
				Map<String, Integer> fieldExpertCountMap= new HashMap<String, Integer>();
				for (Text each : values) {
					String value = each.toString();
					String [] items = value.split(" ");
					String mapKey = items[0]+" "+items[1];
					if(fieldExpertCountMap.containsKey(mapKey)){//判断是否已经有该数值，如有，则将次数加1
						fieldExpertCountMap.put(mapKey, fieldExpertCountMap.get(mapKey).intValue() + Integer.valueOf(items[2]));
	                }else{
	                	fieldExpertCountMap.put(mapKey, Integer.valueOf(items[2]));
	                }
				}
				Iterator<String> iterExpertFieldCountMap = fieldExpertCountMap.keySet().iterator();
				while(iterExpertFieldCountMap.hasNext()) {
					String mapKey = iterExpertFieldCountMap.next();
					context.write(new Text(key), new Text(mapKey+" "+fieldExpertCountMap.get(mapKey)));
				}
			}

		}


		public static class Expert_FieldsReducer
				extends Reducer<Text, Text, Text, Text>{

			@Override
			protected void reduce(Text key, Iterable<Text> values,
					Context context)
					throws IOException, InterruptedException {
				Map<String, Integer> expertFieldCountMap= new HashMap<String, Integer>();
				Map<String, Map<String, Integer>> expertFieldDetailCountMap= new HashMap<String, Map<String, Integer>>();

				for (Text each : values) {
					String value = each.toString();
					String [] items = value.split(" ");
					if(expertFieldCountMap.containsKey(items[0])){//判断是否已经有该数值，如有，则将次数加
						expertFieldCountMap.put(items[0], expertFieldCountMap.get(items[0]).intValue() + Integer.valueOf(items[2]));
	                }else{
	                	expertFieldCountMap.put(items[0], Integer.valueOf(items[2]));
	                	Map<String, Integer> map = new HashMap<String, Integer>();
	                	map.put("paper", 0);
	                	map.put("patent", 0);
	                	map.put("project", 0);
	                	expertFieldDetailCountMap.put(items[0], map);
	                }
					int nowcount = expertFieldDetailCountMap.get(items[0]).get(items[1]);
					expertFieldDetailCountMap.get(items[0]).put(items[1],nowcount+Integer.valueOf(items[2]));
				}
//				HeapSort heap =new HeapSort();
//				expertFieldCountMap = heap.sortMap(expertFieldCountMap, RESULT_COUNT);

				Iterator<String> iterExpertFieldCountMap = expertFieldCountMap.keySet().iterator();


				while(iterExpertFieldCountMap.hasNext()) {
					String field = iterExpertFieldCountMap.next();
		    		Integer fieldCount = expertFieldCountMap.get(field);
		    		Integer papercount = expertFieldDetailCountMap.get(field).get("paper");
		    		Integer patentcount = expertFieldDetailCountMap.get(field).get("patent");
		    		Integer projectcount = expertFieldDetailCountMap.get(field).get("project");
		    		//System.err.println(key+" "+field+" "+papercount+" "+patentcount+" "+projectcount+" "+fieldCount);
		    		context.write(new Text(key), new Text(field+" "+papercount+" "+patentcount+" "+projectcount+" "+fieldCount));
				}
			}
		}
		
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Expert Fields");
			job.setJarByClass(Expert_Fields_v3_1.class);
			job.setMapperClass(Expert_FieldsMaper.class);
			job.setCombinerClass(Expert_FieldsCombiner.class);
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
