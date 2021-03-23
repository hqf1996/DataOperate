package com.autoStep.project;

import com.structure.ProjectStructure;
import com.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 替换老的项目表中的类型
 * @author yhj
 *
 */
public class ChangeOldType {
	public static class ChangeOldTypeMap extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		Map<String, String> projectJihua1Map = null;  //项目与国家项目类型对应
		Map<String, String> projectJihua2Map = null;  //项目与地方项目类型对应
		Map<String, String> projectTypeMap = null;   //项目目录与类型代码对应
		List<String[]> ZJtypeList = null;
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			ZJtypeList = Util.getListFromDir("project_type_change", "\t", 4);
			projectJihua1Map = Util.getMapFromDir("projectJihua.txt", "\t", 0, 1);
			projectTypeMap = Util.getMapFromDir("project_type_map", "\t", 1, 0);
			List<String[]> projectJihua2List = Util.getListFromDir("projectJihua2.txt", "\t", 3);
			projectJihua2Map = new HashMap<String, String>();
			//地方的
			for(String[] infos: projectJihua2List){
				if(infos[1].startsWith(infos[2])){
					projectJihua2Map.put(infos[0], infos[1]);
				}else {
					projectJihua2Map.put(infos[0], infos[2] + infos[1]);
				}
			}
			System.out.println("typeList : " + ZJtypeList.size() + "projectJihua1Map : " + projectJihua1Map.size() 
				+ "projectJihua2Map : "+ projectJihua2Map.size() + "projectTypeMap : "+ projectTypeMap.size()) ;
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			//未归的浙江省级项目
			if(infos[ProjectStructure.project_type_more].equals("333000")){
				System.out.print("change brfore :" + infos[ProjectStructure.PROJECT_ID] + "\t" +
						infos[ProjectStructure.name] + "\t" + infos[ProjectStructure.type] + "\t" + infos[ProjectStructure.project_type_more]);
				for(String[] typeInfo:ZJtypeList){
					if(typeInfo[1].equals(infos[ProjectStructure.type])){
						infos[ProjectStructure.type] = typeInfo[2];
						infos[ProjectStructure.project_type_more] = typeInfo[3];
					}
				}
				System.out.println("\t\tchange after :" + infos[ProjectStructure.PROJECT_ID] + "\t" +
						infos[ProjectStructure.name] + "\t" + infos[ProjectStructure.type] + "\t" + infos[ProjectStructure.project_type_more]);
			}else if (infos[ProjectStructure.project_type_more].equals("5")) {
				System.out.print("change brfore :" + infos[ProjectStructure.PROJECT_ID] + "\t" +
						infos[ProjectStructure.name] + "\t" + infos[ProjectStructure.type] + "\t" + infos[ProjectStructure.project_type_more]);
				String newType = getTypeStr(infos[ProjectStructure.url], infos[ProjectStructure.name], infos[ProjectStructure.type]);
				if(!newType.equals(infos[ProjectStructure.type])){
					infos[ProjectStructure.type] = newType;
					infos[ProjectStructure.project_type_more] = getTypeCode(infos[ProjectStructure.type], "605");
				}else {
					infos[ProjectStructure.type] = "其他纵向";
					infos[ProjectStructure.project_type_more] = "605";
				}
				infos[ProjectStructure.project_type] = infos[ProjectStructure.project_type_more].substring(0, 1);
				System.out.println("\t\tchange after :" + infos[ProjectStructure.PROJECT_ID] + "\t" +
						infos[ProjectStructure.name] + "\t" + infos[ProjectStructure.type] + "\t" + infos[ProjectStructure.project_type_more]);
			}
			outKey.set(StringUtils.join(infos, "\t"));
			context.write(outKey, NullWritable.get());
		}
		
		/**
		 * 根据url和项目名字关联项目类型
		 * @param url
		 * @param name
		 * @return
		 */
		private String getTypeStr (String url, String name, String type) {
			if(projectJihua1Map.containsKey(url)){
				return projectJihua1Map.get(url);
			}else {
				if(projectJihua2Map.containsKey(name)){
					return projectJihua2Map.get(name);
				}else{
					return type;
				}
			}
		}
		
		
		/**
		 * 根据项目类型得到其类型代码
		 * @param key
		 * @return
		 */
		private String getTypeCode(String typeStr, String other) {
			if(projectTypeMap.containsKey(typeStr)){
				return projectTypeMap.get(typeStr);
			}else {
				return other;
			}
		}
		
	}
}
