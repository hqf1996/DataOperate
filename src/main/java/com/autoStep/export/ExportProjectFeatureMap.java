package com.autoStep.export;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.ProjectStructure;

/**
 * 
 * @author yhj
 * 导出项目的特征文档
 */
public class ExportProjectFeatureMap extends Mapper<Object, Text, Text, NullWritable>{
	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String infos[] = value.toString().split("\t");
		boolean flag = false;
		String[] expertIds = {"null", "null", "null", "null"};
		for(int i = 0;i<4;i++){
			int p = ProjectStructure.leader+ 2*i;
			if(!infos[ProjectStructure.first_author_f+i].equals("null") && infos[p].indexOf("编辑部")== -1 && infos[p].indexOf("发明人")== -1 
					&& infos[p].indexOf("课题组")== -1 && infos[p].indexOf("项目组")== -1 && infos[p].indexOf("不公开姓名")== -1){
				expertIds[i] = infos[ProjectStructure.first_author_f+i];
				if(!flag){
					flag = true;
				}
			}
		}
		if(flag){
			if(infos[ProjectStructure.subject_code_f].equals("C")){
				infos[ProjectStructure.subject_code_f] = "C7";
			}
			String result = infos[ProjectStructure.PROJECT_ID] + '\t' + infos[ProjectStructure.subject_code_f] + '\t' + 
							infos[ProjectStructure.type_code_f] + '\t' + infos[ProjectStructure.province_code_f] + '\t' +
							infos[ProjectStructure.unit_code_f] + '\t' + infos[ProjectStructure.rank_code_f] + '\t' +
							infos[ProjectStructure.unit] + '\t' + infos[ProjectStructure.project_type] + '\t' +
							StringUtils.join(expertIds, "\t");
			context.write(new Text(result), NullWritable.get());
		}
	}
}
