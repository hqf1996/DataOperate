package com.autoStep.export;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PaperStructure;
import com.util.Util;

/**
 * @author yhj
 * 导出数据库核心期刊的论文特征向量
 */
public class ExportPaperFeatureMap extends Mapper<Object, Text, Text, NullWritable>{
	Map<String, String> journal = new HashMap<>();
	@Override
	protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		journal = Util.getMapFromDir("paper_journal", 0, 0);
	}
	
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String infos[] = value.toString().split("\t");
//		if(journal.containsKey(infos[PaperStructure.journal_name])){
		if(true){
			String[] expertIds = {"null", "null", "null", "null"};
			boolean flag = false;
			for(int i = 0;i<4;i++){
				int nameP = PaperStructure.first_author+i*4;
				int p = PaperStructure.first_author_baidu + i*4;
				if(!infos[PaperStructure.first_author_f+i].equals("null") && infos[p].equals("01") 
						&& infos[nameP].indexOf("编辑部")== -1 && infos[nameP].indexOf("发明人")== -1
						&& infos[nameP].indexOf("课题组")== -1 && infos[nameP].indexOf("项目组")== -1 && infos[nameP].indexOf("不公开姓名")== -1){
					expertIds[i] = infos[PaperStructure.first_author_f+i];
					if(!flag){
						flag = true;
					}
				}
			}
			if(flag){
				if(infos[PaperStructure.subject_code_f].equals("C")){
					infos[PaperStructure.subject_code_f] = "C7";
				}
				String result = infos[PaperStructure.PAPER_ID] + "\t" + infos[PaperStructure.subject_code_f] + "\t" + 
						infos[PaperStructure.type_code_f] + "\t" + infos[PaperStructure.province_code_f] + "\t" + 
						infos[PaperStructure.unit_code_f] + "\t" + infos[PaperStructure.rank_code_f] + "\t" +
						infos[PaperStructure.unit] + "\t" + infos[PaperStructure.journal_quality] + "\t" +
						StringUtils.join(expertIds, "\t");
//				System.out.println(infos[PaperStructure.journal_name] + ": " + result);
				context.write(new Text(result), NullWritable.get());
			}
		}
	}
}
