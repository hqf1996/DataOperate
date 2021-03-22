package com;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.autoStep.unit.CleanUnit;
import com.util.Util;

public class addAreaTypeCode {
	public static class addAreaTypeCodeMap extends Mapper<Object, Text, Text, NullWritable>{
		Text outKey = new Text();
		Text outvalue = new Text();
		List<String[]> areainfos = null;
		List<String[]> cityinfos = null;
		List<String[]> provinceinfos = null;
		List<String[]> universityinfos = null;
		List<String[]> institueinfos = null;
		CleanUnit cleanUnit = null;
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			areainfos =  Util.getListFromDir("unit_areacode_map_new", "'",4);
			cityinfos =  Util.getListFromDir("unit_citycode_map", "'",3);
			provinceinfos =  Util.getListFromDir("unit_provincecode_map", "'",3);
			universityinfos =  Util.getListFromDir("china_universities", "'",4);
			institueinfos =  Util.getListFromDir("china_institues", "'",4);
			cleanUnit = new CleanUnit(areainfos, cityinfos, provinceinfos, universityinfos, institueinfos);
		}
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("'");
			if(infos.length == 4){
				infos[3] = infos[3] + "'" + cleanUnit.clean(infos[1]).getTypeCode();
				outKey.set(StringUtils.join(infos, "'"));
				context.write(outKey, NullWritable.get());
			}
		}
	}
}
