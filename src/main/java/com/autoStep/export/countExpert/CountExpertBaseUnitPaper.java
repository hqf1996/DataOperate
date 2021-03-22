package com.autoStep.export.countExpert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.autoStep.unit.CleanUnit;

/**
 * 
 * @author yhj
 * 按单位统计专家
 * Step1
 * 从论文表中统计专家发过多少篇文章,各个领域各占多少
 */
public class CountExpertBaseUnitPaper {
	
	public static class CountExpertBaseUnitPaperMap extends Mapper<Object, Text, Text, Text>{
		public Text outValue = new Text();
		public Text outKey = new Text();
		public static final int STRNUM = 53;
		List<String[]> areainfos = null;
		List<String[]> cityinfos = null;
		List<String[]> provinceinfos = null;
		List<String[]> universityinfos = null;
		List<String[]> institueinfos = null;
		CleanUnit cleanUnit = null;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			areainfos =  getlist("unit_areacode_map_new", 3);
			cityinfos =  getlist("unit_citycode_map", 3);
			provinceinfos =  getlist("unit_provincecode_map", 3);
			universityinfos =  getlist("china_universities", 3);
			institueinfos =  getlist("china_institues", 3);
			cleanUnit = new CleanUnit(areainfos, cityinfos, provinceinfos, universityinfos, institueinfos);
		}
		
		public List<String[]> getlist(String dirPath, int num)throws IOException, InterruptedException{
			File file = new File(dirPath);
			List<String[]> list = new ArrayList<String[]>();
			for(String path: file.list()){
				if(path.startsWith("part-")){
					String filePath = dirPath + "/" + path;
					InputStreamReader isr = new InputStreamReader(new FileInputStream(filePath),"utf-8");
					BufferedReader br = new BufferedReader(isr);
					String line;
					while((line = br.readLine()) != null){
						String[] strs = line.split("'");
						if(strs.length >= num){//防止缺少areacode的unit_areacode进入
							list.add(strs);
						}
					}
					br.close();
					isr.close();
				}
				
			}
			return list;
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos.length == STRNUM){
				for(int i = 0;i<4;i++){
					int baidu = 10+4*i;
					int expertIdP = 49+i;
					int expertUnitP = 9+4*i;
					int nameP = 8+4*i;
					String unit = infos[43];
					if(!infos[expertIdP].equals("null") && !infos[nameP].equals("null") && infos[nameP].indexOf("编辑部")== -1 && infos[nameP].indexOf("不公告发明人")== -1){
						if(!infos[expertUnitP].equals("null")){
							unit = infos[expertUnitP];
						}
						outKey.set(infos[expertIdP]);
						if(!unit.equals("null")){
							String typeCode;
							if(unit.equals(infos[43])){
								typeCode = infos[44];
							}else {
								typeCode = cleanUnit.clean(unit, "null", "null").getTypeCode();
							}
							if(infos[baidu].equals("null")){
								outValue.set(infos[expertIdP]+ "\t" +unit + "\t" + infos[42] + "\t99\t" + infos[1] + "\t" + infos[nameP] + "\t" + typeCode);
							}else {
								outValue.set(infos[expertIdP]+ "\t" + unit + "\t" + infos[42] + "\t" + infos[baidu] + "\t" + infos[1] + "\t" + infos[nameP] + "\t" + typeCode); // id unit sunjectCode baidu
							}
							System.out.println(outValue.toString());
							context.write(outKey,outValue);
						}
					}
				}
			}
		}
	}
	
	/*public static class CountExpertBaseUnitPaperReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Map<String, Integer> subjectIndexmap = new HashMap<String, Integer>();
		public Text outText = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int paperSum = 0, firstCount = 0, secondCount = 0;
			String subjectFirst = "Z9";
			String subjectSecond = "Z9";
			String[] infos = null;
			subjectIndexmap.clear();
			for(Text value:values){
				infos = value.toString().split("\t");
				Integer count = subjectIndexmap.get(infos[2]);
				if(count != null){
					subjectIndexmap.put(infos[2], count+1);
				}else {
					subjectIndexmap.put(infos[2], 0);
				}
				paperSum++;
			}
			for(String mapKey:subjectIndexmap.keySet()){ 
				if(subjectIndexmap.get(mapKey) > firstCount){
					secondCount = firstCount;
					firstCount = subjectIndexmap.get(mapKey);
					subjectSecond = subjectFirst;
					subjectFirst = mapKey;
				}else if (subjectIndexmap.get(mapKey) > secondCount) {
					secondCount = subjectIndexmap.get(mapKey);
					subjectSecond = mapKey;
				}
			}
			if(infos != null){
				String result = infos[0] + "\t" + infos[1] + "\t" + paperSum + "\t0\t0\t" + subjectFirst + "\t" + subjectSecond + "\t" +infos[3]; 
				System.out.println(result);
				outText.set(result);
				context.write(outText, NullWritable.get());
			}
		}
	}*/
	
	public static class CountExpertBaseUnitPaperReduce extends Reducer<Text, Text, Text, NullWritable>{
		public Map<String, Integer> subjectIndexMap = new HashMap<String, Integer>();
		public Text outText = new Text();
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			subjectIndexMap.put("A1", 0);
			subjectIndexMap.put("A2", 1);
			subjectIndexMap.put("A3", 2);
			subjectIndexMap.put("A4", 3);
			subjectIndexMap.put("A5", 4);
			subjectIndexMap.put("A9", 5);
			subjectIndexMap.put("C1", 6);
			subjectIndexMap.put("C2", 7);
			subjectIndexMap.put("Z1", 8);
			subjectIndexMap.put("Z9", 9);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int[] subjectCount = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
			int paperCount = 0;
			String[] infos = null;
			for(Text value:values){
				infos = value.toString().split("\t");
				if(subjectIndexMap.containsKey(infos[2])){
					subjectCount[subjectIndexMap.get(infos[2])]++;
				}
				paperCount++;
			}
			if(infos != null){
				StringBuffer subjectCountStr = new StringBuffer();
				for(int i = 0;i<subjectCount.length;i++){
					subjectCountStr.append(subjectCount[i]);
					subjectCountStr.append("\t");
				}
				outText.set(infos[0] + "\t" + infos[1] + "\t" + paperCount + "\t0\t0\t" + subjectCountStr.toString() + infos[3] + "\t" + infos[5] + "\t" + infos[6]);
				context.write(outText, NullWritable.get());
			}
		}
	}
}
