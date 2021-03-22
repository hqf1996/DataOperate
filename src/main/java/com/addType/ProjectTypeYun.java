package com.addType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.autoStep.unit.CleanUnit;
import com.structure.ProjectStructure;
import com.util.Util;

public class ProjectTypeYun {

	public static class addProjecttypeMap extends Mapper<Object, Text, Text, NullWritable>{
		
		List<String[]> areainfos = null;
		List<String[]> cityinfos = null;
		List<String[]> provinceinfos = null;
		List<String[]> universityinfos = null;
		List<String[]> institueinfos = null;
		CleanUnit cleanUnit = null;
		
		Text outKey = new Text();
		Map<String, String> typeCodemap = null;
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			@SuppressWarnings("deprecation")
//			Path[] p = context.getLocalCacheFiles();
			areainfos =  Util.getListFromDir("unit_areacode_map_new", "'",3);
			cityinfos =  Util.getListFromDir("unit_citycode_map", "'",3);
			provinceinfos =  Util.getListFromDir("unit_provincecode_map", "'",3);
			universityinfos =  Util.getListFromDir("china_universities", "'",3);
			institueinfos =  Util.getListFromDir("china_institues", "'",3);			
			typeCodemap = Util.getMapFromDir("project_type_map", "\t", 1, 0);
			cleanUnit = new CleanUnit(areainfos, cityinfos, provinceinfos, universityinfos, institueinfos);
		}
		
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			System.out.println(infos[ProjectStructure.type]);
			System.out.println("map size:" + typeCodemap.size());
			if(infos[ProjectStructure.type].equals("null") || infos[ProjectStructure.type].equals("")){
				infos[ProjectStructure.source] = infos[ProjectStructure.source] + "\t" + "6" + "\t" + "699";
			}else if (typeCodemap.containsKey(infos[ProjectStructure.type])) {
				String typeCode = typeCodemap.get(infos[ProjectStructure.type]);
				infos[ProjectStructure.source] = infos[ProjectStructure.source] + "\t" + typeCode.charAt(0) + "\t" + typeCode;
			}else {
				infos[ProjectStructure.source] = infos[ProjectStructure.source] + "\tnull\tnull";
			}
			infos[ProjectStructure.source] = infos[ProjectStructure.source] + "\t" + infos[ProjectStructure.source+3];
			infos[ProjectStructure.source+5] = cleanUnit.clean(infos[ProjectStructure.unit]).getUnitCode();
			infos[ProjectStructure.id] = "null";
			outKey.set(StringUtils.join(infos,"\t"));
			context.write(outKey, NullWritable.get());
		}
	} 
}
