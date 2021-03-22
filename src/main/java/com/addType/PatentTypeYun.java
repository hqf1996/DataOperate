package com.addType;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.autoStep.unit.CleanUnit;
import com.structure.PatentStructure;
import com.util.Util;

public class PatentTypeYun {
	
	
	public static class AddPatentTypeMap extends Mapper<Object, Text, Text, NullWritable>{
		List<String[]> areainfos = null;
		List<String[]> cityinfos = null;
		List<String[]> provinceinfos = null;
		List<String[]> universityinfos = null;
		List<String[]> institueinfos = null;
		CleanUnit cleanUnit = null;
		
		HashSet<Character> typeSet = new HashSet<Character>();
		String application_no = null;
		String type = null;
		Text outKey = new Text();
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
			
			typeSet.add('1');typeSet.add('2');typeSet.add('3');
			typeSet.add('4');typeSet.add('8');typeSet.add('9');
			
			cleanUnit = new CleanUnit(areainfos, cityinfos, provinceinfos, universityinfos, institueinfos);
		}
		
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
			String[] infos = value.toString().split("\t");
			application_no = infos[PatentStructure.application_no];
			if(typeSet.contains(application_no.charAt(6))){
				type = String.valueOf(application_no.charAt(6));
			}else {
				type = "4";
			}
			infos[0] = "null";
			infos[PatentStructure.url] = infos[PatentStructure.url] + "\t" + type + "\t" + infos[PatentStructure.url+3];
			infos[PatentStructure.url+5] = cleanUnit.clean(infos[PatentStructure.applicant]).getUnitCode();
			outKey.set(StringUtils.join(infos, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
