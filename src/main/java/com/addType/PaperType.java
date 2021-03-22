package com.addType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PaperStructure;

public class PaperType {

	public static class AddPaperTypeMap extends Mapper<Object, Text, Text, NullWritable>{
		Map<String, String> journalinfos = null;
		Map<String, String> journalQuailtyMap = new HashMap<String, String>();
		Text outKey = new Text();
		
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			journalinfos = getMap("journal_information", "\t", 1, 5);
			journalQuailtyMap.put("SCI", "1");
			journalQuailtyMap.put("FIRST", "2");
			journalQuailtyMap.put("EI", "3");
			journalQuailtyMap.put("HEXIN", "4");
			journalQuailtyMap.put("OTHERS", "5");
			journalQuailtyMap.put("null", "7");
		}
		
		public Map<String, String> getMap(String dirPath, String splitStr,int keyNum, int valueNum) throws IOException{
			File file = new File(dirPath);
			int maxNum = keyNum>valueNum?keyNum:valueNum;
			Map<String, String> map = new HashMap<String, String>(); 
			for(String path: file.list()){
				if(path.startsWith("part-")){
					String filePath = dirPath + "/" + path;
					InputStreamReader isr = new InputStreamReader(new FileInputStream(filePath),"utf-8");
					BufferedReader br = new BufferedReader(isr);
					String line;
					while((line = br.readLine()) != null){
						String[] strs = line.split(splitStr);
						if(strs.length > maxNum){//防止数组index越界
							map.put(strs[keyNum], strs[valueNum]);
						}
					}
					br.close();
					isr.close();
				}
			}
			return map;
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
			}
			infos[PaperStructure.flag] = infos[PaperStructure.flag] + "\t" + infos[PaperStructure.flag+3]; 
			outKey.set(StringUtils.join(infos, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
