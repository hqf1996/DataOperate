package com.updateTableUnit;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.ExpertStructure;
import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.structure.UniversityShortCallStructure;
import com.unitProcess.UnitProcess;
import com.util.Util;

/**
 * Step2 提取出科技厅和教师库专家，顺带处理其单位
 * @author yhj
 *
 */
public class ExportExpertOut {
	public static class ExportExpertOutMap extends Mapper<Object, Text, Text, NullWritable>{
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
		Map<String, String> universityNameMap;
		Text outKey = new Text(); 
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			universityNameMap = Util.getMapFromDir("university_change_name", "'", 0, 1);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[ExpertStructure.source].equals("1") || infos[ExpertStructure.source].equals("5")){
				if(infos[ExpertStructure.organization].equals("null")){
					infos[ExpertStructure.organization] = infos[ExpertStructure.unit];
				}
				infos[ExpertStructure.unit] = unitProcess.dispose(infos[ExpertStructure.unit]).getUnit();
				if(!infos[ExpertStructure.unit].equals("null")){
					if(universityNameMap.containsKey(infos[ExpertStructure.unit])){
						infos[ExpertStructure.unit] = universityNameMap.get(infos[ExpertStructure.unit]);
					}
					outKey.set(StringUtils.join(infos, "\t"));
					context.write(outKey, NullWritable.get());
				}
			}
		}
	} 
}
