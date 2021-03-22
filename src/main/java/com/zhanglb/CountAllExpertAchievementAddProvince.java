package com.zhanglb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.structure.UniversityShortCallStructure;
import com.unitProcess.UnitProcess;
import com.util.Util;

/**
 * 给表补上省份信息（后来加的）
 * 输入数据 来自step4和unitMap产生的结果
 * step5
 * @author yhj
 *
 */
public class CountAllExpertAchievementAddProvince {
	public static class CountAllExpertAchievementAddProvinceMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String filepath = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(filepath.contains("unitMap")){
				outKey.set(infos[1]);
				outValue.set(infos[3]);
				context.write(outKey, outValue);
			}else {
				outKey.set(infos[2]);
				context.write(outKey, value);
			}
		}
	}
	
	public static class CountAllExpertAchievementAddProvinceReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String provinceCode = null;
			List<String[]> list = new ArrayList<String []>();
			for(Text value:values){
				String[] infos = value.toString().split("\t");
				if(infos.length == 1){
					provinceCode = infos[0];
				}
				if(infos.length == 11){
					list.add(infos);
				}
			}
			if(!list.isEmpty()){
				if(provinceCode == null){
					provinceCode = unitProcess.dispose(list.get(0)[2]).getProvinceCode();
					System.out.println("程序处理：" + list.get(0)[2] + "\t" + provinceCode);
				}else {
					System.out.println("映射处理：" + list.get(0)[2] + "\t" + provinceCode);
				}
				for(String[] infos:list){
					infos[3] = infos[3] + "\t" + provinceCode;
					outKey.set(StringUtils.join(infos, "\t"));
					context.write(outKey, NullWritable.get());
				}
			}
		}
	}
}
