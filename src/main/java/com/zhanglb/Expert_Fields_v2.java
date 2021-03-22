package com.zhanglb;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.record.compiler.JBoolean;


/**
 * 浠ラ鍩熶负绮掑害锛屾垒鍒拌棰嗗烟涓嬫垚鏋沧渶澶氱殑54涓笓瀹?
 * 
 * 璇ョ被鐢ㄤ簬璁＄畻    鍗曚綅锛堜笁涓┖镙硷级涓揿1ID(涓€涓┖镙?涓揿1flag锛堜袱涓┖镙硷级涓揿2ID(涓€涓┖镙?涓揿2flag...锛堜袱涓┖镙硷级涓揿nID(涓€涓┖镙?涓揿nflag锛堜笁涓┖镙硷级鍒呜瘝1锛堜竴涓┖镙硷级鍒呜瘝2锛堜竴涓┖镙硷级鍒呜瘝3...     琛ㄤ腑鎻愬彇淇℃伅
 * 杈揿嚭涓?
 * 涓揿ID 棰嗗烟1 璁烘枃鏁?涓揿埄鏁?椤圭洰鏁?璇ラ鍩熶笅镐绘暟
 * 涓揿ID 棰嗗烟2 璁烘枃鏁?涓揿埄鏁?椤圭洰鏁?璇ラ鍩熶笅镐绘暟
 * 涓揿ID 棰嗗烟3 璁烘枃鏁?涓揿埄鏁?椤圭洰鏁?璇ラ鍩熶笅镐绘暟
 *  镙煎纺
 *  姣忎竴涓鍩熸渶澶氭湁54鏉′笉鍚屼笓瀹剁殑璁板綍 
 *  
 * */
public class Expert_Fields_v2 {
	
	final static int RESULT_COUNT=54;//濮ｅ繋閲沧０鍡楃厵閺堬拷婢舵氨娈戞稉鎾愁啀阎ㄥ嫭鏆熼柌锟?
	//final static int RESULT_RANGE_MIN=1;//鏉╂柨娲栫紒鎾寸亯娑掳拷 閺堢儤鐎拠鍕瀻阎ㄥ嫯瀵栭崶锟?
	//final static int RESULT_RANGE_MAX=100;
	
	public static class Expert_FieldsMaper extends
			Mapper<Object, Text, Text, Text>{

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
				String filepath = ((FileSplit)context.getInputSplit()).getPath().toString();//阏惧嘲褰囱ぐ鎾冲阉垮秳缍旈惃鍕攽娴犲骸鎽㈡稉颜呮瀮娴犺埖娼?
				String type = new String();
				if(filepath.contains("patent")){
					type = "patent";
				}else if(filepath.contains("paper")){
					type = "paper";
				}else if(filepath.contains("project")){
					type = "project";
				}
				String line = value.toString();
				String [] items = line.split("   ");
				if(items.length<3) return;
				//String [] experts = items[1].split("  ");
				//娑揿瓨妞傞敍宀€鐡戞潏鎾冲弪閺傚洣娆㈤弽镦庣础濮濓絿钬橀崥搴㈡禌阉广垹娲栨稉濠囨桨娑掳拷鐞涳拷
				String [] expertsTemp = items[1].split(" ");
				String [] experts = new String[expertsTemp.length/2];
				for (int i = 0; i < experts.length; i++) {
					experts[i] = expertsTemp[i*2];
				}
				//end
				String [] fields = items[2].split(" ");
				for (int i = 0; i < fields.length; i++) {
					for (int j = 0; j < experts.length; j++) {
						String [] expertInfo = experts[j].split(" ");
						String EID = expertInfo[0];
						//System.err.println(EID+" "+fields[i]+" "+type+" 1");
						context.write(new Text(fields[i]), new Text(EID+" "+type+" 1"));
					}
				}
		}
	}
	
		public static class Expert_FieldsCombiner 
				extends Reducer<Text, Text, Text, Text>{

			@Override
			protected void reduce(Text key, Iterable<Text> values,
					Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				Map<String, Integer> fieldExpertCountMap= new HashMap<String, Integer>();
				for (Text each : values) {
					String value = each.toString();
					String [] items = value.split(" ");
					String mapKey = items[0]+" "+items[1];
					if(fieldExpertCountMap.containsKey(mapKey)){//閸掋幸镆囬弰顖氭儊瀹歌尙绮￠张澶庮嚉閺佹澘锟界》绱濇俊鍌涙箒阌涘苯鍨亸鍡橆偧閺佹澘濮?
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
					Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				Map<String, Integer> fieldExpertCountMap= new HashMap<String, Integer>();
				Map<String, Map<String, Integer>> fieldExpertDetailCountMap= new HashMap<String, Map<String, Integer>>();
				//System.err.println(key);
//				for (Text each : values) {
//					System.out.println(each);
//				}
				for (Text each : values) {
					String value = each.toString();
					String [] items = value.split(" ");
					if(fieldExpertCountMap.containsKey(items[0])){//閸掋幸镆囬弰顖氭儊瀹歌尙绮￠张澶庮嚉閺佹澘锟界》绱濇俊鍌涙箒阌涘苯鍨亸鍡橆偧閺佹澘濮?
						fieldExpertCountMap.put(items[0], fieldExpertCountMap.get(items[0]).intValue() + Integer.valueOf(items[2]));
	                }else{
	                	fieldExpertCountMap.put(items[0], Integer.valueOf(items[2]));
	                	Map<String, Integer> map = new HashMap<String, Integer>();
	                	map.put("paper", 0);
	                	map.put("patent", 0);
	                	map.put("project", 0);
	                	fieldExpertDetailCountMap.put(items[0], map);
	                }
					int nowcount = fieldExpertDetailCountMap.get(items[0]).get(items[1]);
					fieldExpertDetailCountMap.get(items[0]).put(items[1],nowcount+Integer.valueOf(items[2]));
				}
				HeapSort heap =new HeapSort();
				fieldExpertCountMap = heap.sortMap(fieldExpertCountMap, RESULT_COUNT);
				
				Iterator<String> iterExpertFieldCountMap = fieldExpertCountMap.keySet().iterator(); 
				
				
				while(iterExpertFieldCountMap.hasNext()) {
					String EID = iterExpertFieldCountMap.next(); 
		    		Integer expertCount = fieldExpertCountMap.get(EID);
		    		Integer papercount = fieldExpertDetailCountMap.get(EID).get("paper");
		    		Integer patentcount = fieldExpertDetailCountMap.get(EID).get("patent");
		    		Integer projectcount = fieldExpertDetailCountMap.get(EID).get("project");
		    		//System.err.println(key+" "+field+" "+papercount+" "+patentcount+" "+projectcount+" "+fieldCount);
		    		context.write(new Text(EID), new Text(key+" "+papercount+" "+patentcount+" "+projectcount+" "+expertCount));
				}
				fieldExpertCountMap = null;
				fieldExpertDetailCountMap = null;
				System.gc();
			}
		}
		
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Expert Fields");
			job.setJarByClass(Expert_Fields_v2.class);
			job.setMapperClass(Expert_FieldsMaper.class);
			job.setCombinerClass(Expert_FieldsCombiner.class);
			job.setReducerClass(Expert_FieldsReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			//org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path("hdfs://10.1.13.111:8020/user/wuyc/zhanglb/PaperJoinWord/output/part-r-00000"));
			org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPaths(job, "hdfs://10.1.13.111:8020/user/zhanglb/fieldFindExperts/input/paper"
					+ ",hdfs://10.1.13.111:8020/user/zhanglb/fieldFindExperts/input/patent"//娑撴挸鍩勯崪宀冾啈閺傚洨娈戠紒鎾寸亯
					+ ",hdfs://10.1.13.111:8020/user/zhanglb/fieldFindExperts/input/project");
			FileOutputFormat.setOutputPath(job, new Path("hdfs://10.1.13.111:8020/user/zhanglb/fieldFindExperts/output"));
			System.exit(job.waitForCompletion(true)?0:1);
			
		}
			
	}
