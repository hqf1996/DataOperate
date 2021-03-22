package com.other;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PaperStructure;
import com.structure.UnitCityCodeMapStructure;
import com.structure.UnitStructure;
import com.structure.UniversityShortCallStructure;
import com.unitProcess.UnitInfo;
import com.unitProcess.UnitProcess;
import com.util.Util;

public class paperSpecialItem {
	public static class paperSpecialItemMap extends Mapper<Object, Text, Text, NullWritable>{
		List<String[]> unitList;
		List<String[]> cityList;
		List<String[]> shortCallList;
		UnitProcess unitProcess;
		Text keyOut = new Text();
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			unitList = Util.getListFromDir("unit", "'", UnitStructure.totalNum);
			cityList = Util.getListFromDir("unit_citycode_map", "'", UnitCityCodeMapStructure.totalNum);
			shortCallList = Util.getListFromDir("university_short_call", "'", UniversityShortCallStructure.totalNum);
			unitProcess = new UnitProcess(unitList, cityList, shortCallList);
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(!infos[PaperStructure.unit].equals("null") && infos[PaperStructure.first_organization].equals("null")){
				infos[PaperStructure.first_organization] = infos[PaperStructure.unit];
				UnitInfo unitInfo = unitProcess.dispose(infos[PaperStructure.unit]);
				infos[PaperStructure.unit] = unitInfo.getUnit();
				infos[PaperStructure.unit_type] = unitInfo.getTypeCode();
				keyOut.set(infos[PaperStructure.PAPER_ID] + "\t" + infos[PaperStructure.first_organization] + "\t" + infos[PaperStructure.unit] + "\t" 
						+ infos[PaperStructure.unit_type] + "\t" + unitInfo.toString());
				context.write(keyOut, NullWritable.get());
			}
		}
	}
}
