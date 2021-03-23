package com.autoStep.project;

import com.structure.ProjectStructure;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * 更新：跟新原项目的数据，有相同的项目成果是将原来的项目字段更新,如果没有直接创建新的uuid添加
 * Pretreatment*和mysqlOut/project_new_unit_subject_code_join_all_expert
 * 数据产生：step1
 * @author yhj
 *
 */
public class Step1Update {
	public static class Step1Map extends Mapper<Object, Text, Text, Text> {
		Text outKey = new Text();
		Text outValue = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			infos[ProjectStructure.name] = infos[ProjectStructure.name].trim();
			for(int i = 0;i<4;i++){
				infos[ProjectStructure.leader + 2*i] = infos[ProjectStructure.leader + 2*i].trim().replaceAll("^\\)", "");
			}
			outKey.set(infos[ProjectStructure.name] + infos[ProjectStructure.leader]);
			outValue.set(StringUtils.join(infos, "\t"));
			context.write(outKey, outValue);
		}
	}

	public static class Step1Reduce extends Reducer<Text, Text, Text, NullWritable> {
		Text outKey = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<String[]> newDatas = new ArrayList<String[]>();
			// String[] newData = null;
			String[] oldData = null;
			for (Text value : values) {
				String[] infos = value.toString().split("\t");
				if (infos.length == (ProjectStructure.fourth_author_f - 3)) {
					newDatas.add(infos);
				}
				if (infos.length == (ProjectStructure.fourth_author_f + 1)) {
					oldData = infos;
				}
			}
			if (oldData != null) {
				if (newDatas.size() > 0) {
					for (String[] newData : newDatas) {
						for (int i = 0; i < newData.length; i++) {// 将不为空的值赋值给老数据
							if (i == ProjectStructure.keywords_ch || i == ProjectStructure.keywords_en
									|| i == ProjectStructure.abstract_ch || i == ProjectStructure.abstract_en) {
								if (!newData[i].equals("null") && !newData[i].equals("")
										&& (oldData[i].equals("null") || oldData[i].equals(""))) {
									oldData[i] = newData[i];
								}
							} else {
								if (!newData[i].equals("null") && !newData[i].equals("")) {
									oldData[i] = newData[i];
								}
							}
						}
					}
					System.out.println(StringUtils.join(oldData, "\t"));
				}
				oldData = Arrays.copyOf(oldData, ProjectStructure.fourth_author_f - 3);
				outKey.set(StringUtils.join(oldData, "\t"));
				context.write(outKey, NullWritable.get());
			}else {
				String[] newData = newDatas.get(0);
				newData[ProjectStructure.PROJECT_ID] = UUID.randomUUID().toString();
				String result = StringUtils.join(newData, "\t");
				System.out.println("newData Data :" + result);
				outKey.set(result);
				context.write(outKey, NullWritable.get());
			}
		}
	}
}
