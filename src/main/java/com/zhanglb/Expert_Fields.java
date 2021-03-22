package com.zhanglb;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
 * 镨囥儳琚凄銊ょ艾镨侊纰鐣?   閸楁洑缍呴敍鍫灭瑏娑擃亚铍栭弽纭风礆娑撴挸顔?ID(娑掳拷娑擃亚铍栭弽锟?娑撴挸顔?flag阌涘牅琚辨稉颜嗏敄閺岖》绱氭稉鎾愁啀2ID(娑掳拷娑擃亚铍栭弽锟?娑撴挸顔?flag...阌涘牅琚辨稉颜嗏敄閺岖》绱氭稉鎾愁啀nID(娑掳拷娑擃亚铍栭弽锟?娑撴挸顔峮flag阌涘牅绗佹稉颜嗏敄閺岖》绱氶崚鍡氱槤1阌涘牅绔存稉颜嗏敄閺岖》绱氶崚鍡氱槤2阌涘牅绔存稉颜嗏敄閺岖》绱氶崚鍡氱槤3...     鐞涖劋鑵戦幓镒絿娣団剥浼?
 * 鏉堟挸锸稉锟?
 * 娑撴挸顔岻D 妫板枣镦? 镨佺儤鏋冮弫锟?娑撴挸鍩勯弫锟?妞ゅ湱娲伴弫锟?镨囥儵顣崺镡剁瑓阉粯鏆?
 * 娑撴挸顔岻D 妫板枣镦? 镨佺儤鏋冮弫锟?娑撴挸鍩勯弫锟?妞ゅ湱娲伴弫锟?镨囥儵顣崺镡剁瑓阉粯鏆?
 * 娑撴挸顔岻D 妫板枣镦? 镨佺儤鏋冮弫锟?娑撴挸鍩勯弫锟?妞ゅ湱娲伴弫锟?镨囥儵顣崺镡剁瑓阉粯鏆?
 *  閺岖厧绱?
 *  濮ｅ繋绔存稉颜幂莹鐎硅埖娓舵径姘箒10閺夆€茬瑝閸氩矂顣崺镡烘畱镨佹澘缍?
 *  
 * */
public class Expert_Fields {
	
	final static int RESULT_COUNT=10;//濮ｅ繋閲沧稉鎾愁啀閺堬拷婢舵氨娈戞０鍡楃厵阎ㄥ嫭鏆熼柌锟?
	//final static int RESULT_RANGE_MIN=1;//鏉╂柨娲栫紒鎾寸亯娑掳拷 閺堢儤鐎拠鍕瀻阎ㄥ嫯瀵栭崶锟?
	//final static int RESULT_RANGE_MAX=100;
	
	public static class Word_UnitMaper extends
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
				//镙规嵁鍒嗛殧绗︽彁鍙栧嚭涓揿镄刬d
				String [] expertsTemp = items[1].split(" ");
				String [] experts = new String[expertsTemp.length/2];
				for (int i = 0; i < experts.length; i++) {
					experts[i] = expertsTemp[i*2];
				}
				//end
				String [] fields = items[2].split(" ");
				
				//鐢变簬鍒呜瘝瀛桦湪鐩稿悓镄勮瘝锛屼娇鐢╯et铡婚櫎鐩稿悓镄勮瘝
				Set<String> fieldSet = new HashSet(Arrays.asList(fields));		
				fields = fieldSet.toArray(new String[fieldSet.size()]);
				//
				
				for (int i = 0; i < fields.length; i++) {
					for (int j = 0; j < experts.length; j++) {
						String [] expertInfo = experts[j].split(" ");
						String EID = expertInfo[0];
						//System.err.println(EID+" "+fields[i]+" "+type+" 1");
						context.write(new Text(EID), new Text(fields[i]+" "+type));
					}
				}
		}
	}
	
		public static class Word_UnitReducer 
				extends Reducer<Text, Text, Text, Text>{

			@Override
			protected void reduce(Text key, Iterable<Text> values,
					Reducer<Text, Text, Text, Text>.Context context)
					throws IOException, InterruptedException {
				Map<String, Integer> expertFieldCountMap= new HashMap<String, Integer>();
				Map<String, Map<String, Integer>> expertFieldDetailCountMap= new HashMap<String, Map<String, Integer>>();
				//System.err.println(key);
//				for (Text each : values) {
//					System.out.println(each);
//				}
				for (Text each : values) {
					String value = each.toString();
					String [] items = value.split(" ");
					if(expertFieldCountMap.containsKey(items[0])){//閸掋幸镆囬弰顖氭儊瀹歌尙绮￠张澶庮嚉閺佹澘锟界》绱濇俊鍌涙箒阌涘苯鍨亸鍡橆偧閺佹澘濮?
						expertFieldCountMap.put(items[0], expertFieldCountMap.get(items[0]).intValue() + 1);
	                }else{
	                	expertFieldCountMap.put(items[0], 1);
	                	Map<String, Integer> map = new HashMap<String, Integer>();
	                	map.put("paper", 0);
	                	map.put("patent", 0);
	                	map.put("project", 0);
	                	expertFieldDetailCountMap.put(items[0], map);
	                }
					int nowcount = expertFieldDetailCountMap.get(items[0]).get(items[1]);
					expertFieldDetailCountMap.get(items[0]).put(items[1],nowcount+1);
				}
				HeapSort heap =new HeapSort();
				expertFieldCountMap = heap.sortMap(expertFieldCountMap, RESULT_COUNT);
				
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
			job.setJarByClass(Expert_Fields.class);
			job.setMapperClass(Word_UnitMaper.class);
			//job.setCombinerClass(Word_UnitReducer.class);
			job.setReducerClass(Word_UnitReducer.class);
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
