package com.other;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;
import com.util.Util;

public class ChangeRepeateUnit {
	public static class PaperChangeRepeateUnitMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		Map<String, String[]> unitMap = new HashMap<String, String[]>();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> unitInfos = Util.getListFromDir("unit_repeat", "\t", 5);
			for(String[] info:unitInfos){
				unitMap.put(info[0], info);
			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			boolean flag = false;
			if(unitMap.containsKey(infos[PaperStructure.unit])){
				String[] unitInfos = unitMap.get(infos[PaperStructure.unit]);
				infos[PaperStructure.province_code_f] = unitInfos[1];
				infos[PaperStructure.unit_code_f] = unitInfos[2];
				infos[PaperStructure.type_code_f] = unitInfos[3];
				infos[PaperStructure.rank_code_f] = unitInfos[4];
				flag = true;
			}
			for(int i = 0;i<4;i++){
				if((!infos[PaperStructure.first_author_unit + i*4].equals("null")) && unitMap.containsKey(infos[PaperStructure.first_author_unit + i*4])){
					infos[PaperStructure.first_author_rank_code + i] = unitMap.get(infos[PaperStructure.first_author_unit + i*4])[4];
					flag = true;
				}
			}
			String result = StringUtils.join(infos, "\t");
			if(flag == true){
				System.out.println(infos[PaperStructure.unit] + ":\t" +  result);
			}
			outKey.set(infos[PaperStructure.id]);
			outValue.set(result);
			context.write(outKey, outValue);
		}
	}
	
	
	public static class PatentChangeRepeateUnitMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		Map<String, String[]> unitMap = new HashMap<String, String[]>();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> unitInfos = Util.getListFromDir("unit_repeat", "\t", 5);
			for(String[] info:unitInfos){
				unitMap.put(info[0], info);
			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			boolean flag = false;
			if(unitMap.containsKey(infos[PatentStructure.applicant])){
				String[] unitInfos = unitMap.get(infos[PatentStructure.applicant]);
				infos[PatentStructure.province_code_f] = unitInfos[1];
				infos[PatentStructure.unit_code_f] = unitInfos[2];
				infos[PatentStructure.type_code_f] = unitInfos[3];
				infos[PatentStructure.rank_code_f] = unitInfos[4];
				flag = true;
			}
			for(int i = 0;i<4;i++){
				if((!infos[PatentStructure.inventor_unit + i*2].equals("null")) && unitMap.containsKey(infos[PatentStructure.inventor_unit + i*2])){
					infos[PatentStructure.first_author_rank_code + i] = unitMap.get(infos[PatentStructure.inventor_unit + i*2])[4];
					flag = true;
				}
			}
			String result = StringUtils.join(infos, "\t");
			if(flag == true){
				System.out.println(infos[PatentStructure.applicant] + ":\t" +  result);
			}
			outKey.set(infos[PatentStructure.id]);
			outValue.set(result);
			context.write(outKey, outValue);
		}
	}
	
	public static class ProjectChangeRepeateUnitMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		Map<String, String[]> unitMap = new HashMap<String, String[]>();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> unitInfos = Util.getListFromDir("unit_repeat", "\t", 5);
			for(String[] info:unitInfos){
				unitMap.put(info[0], info);
			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			boolean flag = false;
			if(unitMap.containsKey(infos[ProjectStructure.unit])){
				String[] unitInfos = unitMap.get(infos[ProjectStructure.unit]);
				infos[ProjectStructure.province_code_f] = unitInfos[1];
				infos[ProjectStructure.unit_code_f] = unitInfos[2];
				infos[ProjectStructure.type_code_f] = unitInfos[3];
				infos[ProjectStructure.rank_code_f] = unitInfos[4];
				flag = true;
			}
			for(int i = 0;i<4;i++){
				if((!infos[ProjectStructure.leader_unit + i*2].equals("null")) && unitMap.containsKey(infos[ProjectStructure.leader_unit + i*2])){
					infos[ProjectStructure.first_author_rank_code + i] = unitMap.get(infos[ProjectStructure.leader_unit + i*2])[4];
					flag = true;
				}
			}
			String result = StringUtils.join(infos, "\t");
			if(flag == true){
				System.out.println(infos[ProjectStructure.unit] + ":\t" +  result);
			}
			outKey.set(infos[ProjectStructure.id]);
			outValue.set(result);
			context.write(outKey, outValue);
		}
	}
	
	public static class ChangeRepeateUnitReduce extends Reducer<Text, Text, Text, NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value:values){
				context.write(value, NullWritable.get());
			}
		}
	}
	
	public static class CountChangeRepeateUnitMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		Map<String, String[]> unitMap = new HashMap<String, String[]>();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> unitInfos = Util.getListFromDir("unit_repeat", "\t", 5);
			for(String[] info:unitInfos){
				unitMap.put(info[0], info);
			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			boolean flag = false;
			if(unitMap.containsKey(infos[2])){
				String[] unitInfos = unitMap.get(infos[2]);
				infos[3] = unitInfos[3];
				infos[4] = unitInfos[1];
				flag = true;
			}
			if(flag){
				System.out.println(infos[2] + "\t" + infos[3] + "\t" + infos[4]);
			}
			String result = StringUtils.join(infos, "\t");
			outKey.set(infos[0]);
			outValue.set(result);
			context.write(outKey, outValue);
		}
	}
	
	public static class FieldFindExpertsChangeRepeateUnitMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		Map<String, String[]> unitMap = new HashMap<String, String[]>();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> unitInfos = Util.getListFromDir("unit_repeat", "\t", 5);
			for(String[] info:unitInfos){
				unitMap.put(info[0], info);
			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			boolean flag = false;
			if(unitMap.containsKey(infos[3])){
				String[] unitInfos = unitMap.get(infos[3]);
				infos[4] = unitInfos[3];
				infos[5] = unitInfos[1];
				flag = true;
			}
			if(flag){
				System.out.println(infos[3] + "\t" + infos[4] + "\t" + infos[5]);
			}
			String result = StringUtils.join(infos, "\t");
			outKey.set(infos[0]);
			outValue.set(result);
			context.write(outKey, outValue);
		}
	}
	
	public static class UnitMapChangeRepeateUnitMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		Text outValue = new Text();
		Map<String, String[]> unitMap = new HashMap<String, String[]>();
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> unitInfos = Util.getListFromDir("unit_repeat", "\t", 5);
			for(String[] info:unitInfos){
				unitMap.put(info[0], info);
			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			boolean flag = false;
			if(unitMap.containsKey(infos[1])){
				String[] unitInfos = unitMap.get(infos[1]);
				infos[2] = unitInfos[3];
				infos[3] = unitInfos[1];
				infos[4] = unitInfos[2];
				infos[5] = unitInfos[4];
				flag = true;
			}
			String result = StringUtils.join(infos, "\t");
			if(flag){
				System.out.println(result);
			}
			outKey.set(infos[0]);
			outValue.set(result);
			context.write(outKey, outValue);
		}
	}
}
