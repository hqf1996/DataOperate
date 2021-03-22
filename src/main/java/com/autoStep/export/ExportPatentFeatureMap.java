package com.autoStep.export;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.PatentStructure;

/**
 * 
 * @author yhj
 * 导出专利的特征文档
 */
public class ExportPatentFeatureMap extends Mapper<Object, Text, Text, NullWritable>{
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String infos[] = value.toString().split("\t");
//		if(infos[PatentStructure.application_no].charAt(6) == '1'){ //导出发明专利
		if(true){
			String[] expertIds = {"null", "null", "null", "null"};
			boolean flag = false;
			for(int i = 0;i<4;i++){
				int p = PatentStructure.inventor+2*i;
				if(!infos[PatentStructure.first_author_f+i].equals("null") && infos[p].indexOf("编辑部")== -1 && infos[p].indexOf("发明人")== -1 
						&& infos[p].indexOf("课题组")== -1 && infos[p].indexOf("项目组")== -1 && infos[p].indexOf("不公开姓名")== -1){
					expertIds[i] = infos[PatentStructure.first_author_f+i];
					if(!flag){
						flag = true;
					}
				}
			}
			if(flag){
				if(infos[PatentStructure.subject_code_f].equals("C")){
					infos[PatentStructure.subject_code_f] = "C7";
				}
				String result = infos[PatentStructure.PATENT_ID] + '\t' + infos[PatentStructure.subject_code_f] + '\t' + 
						infos[PatentStructure.type_code_f] + '\t' + infos[PatentStructure.province_code_f] + '\t' + 
						infos[PatentStructure.unit_code_f] + '\t' + infos[PatentStructure.rank_code_f] + '\t' +
						infos[PatentStructure.applicant] + '\t' + infos[PatentStructure.patent_type] + '\t' +
						StringUtils.join(expertIds, "\t");
				context.write(new Text(result), NullWritable.get());
			}
		}
	}
}
