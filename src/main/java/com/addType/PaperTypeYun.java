package com.addType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.autoStep.unit.CleanUnit;
import com.structure.PaperStructure;
import com.util.Util;

public class PaperTypeYun {
	
	public static class AddPaperTypeMap extends Mapper<Object, Text, Text, NullWritable>{
		List<String[]> areainfos = null;
		List<String[]> cityinfos = null;
		List<String[]> provinceinfos = null;
		List<String[]> universityinfos = null;
		List<String[]> institueinfos = null;
		CleanUnit cleanUnit = null;
		
		Map<String, String> journalinfos = null;
		Map<String, String> journalQuailtyMap = new HashMap<String, String>();
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
			
			journalinfos = Util.getMapFromDir("journal_information", "\t", 1, 5);
			journalQuailtyMap.put("SCI", "1");
			journalQuailtyMap.put("FIRST", "2");
			journalQuailtyMap.put("EI", "3");
			journalQuailtyMap.put("HEXIN", "4");
			journalQuailtyMap.put("OTHERS", "5");
			
			cleanUnit = new CleanUnit(areainfos, cityinfos, provinceinfos, universityinfos, institueinfos);
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(!infos[PaperStructure.journal_name].equals("null")){
				if(journalinfos.containsKey(infos[PaperStructure.journal_name])){
					String newJournalQuality = journalQuailtyMap.get(journalinfos.get(infos[PaperStructure.journal_name]));
					if (newJournalQuality != null){
						infos[PaperStructure.journal_quality] = newJournalQuality;
					}else {
						infos[PaperStructure.journal_quality] = "6";
					}
				}else {
					infos[PaperStructure.journal_quality] = "6";
				}
			}else {
				infos[PaperStructure.journal_quality] = "7";
			}
			infos[PaperStructure.flag] = infos[PaperStructure.flag] + "\t" + infos[PaperStructure.flag+3]; 
			infos[PaperStructure.flag+5] =  cleanUnit.clean(infos[PaperStructure.unit]).getUnitCode();
			infos[PaperStructure.id] = null;
			outKey.set(StringUtils.join(infos, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
