package com.autoStep.project;

import com.structure.ProjectStructure;
import com.util.Util;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * step3 提取出专家的名字和单位
 * 输入 step2，输出step3
 * @author yhj
 */
public class Step3 {
	public static class Step3Map extends Mapper<Object, Text, Text, NullWritable>{
		Map<String, String> universityNameMap;
		Text outKey = new Text();
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			universityNameMap = Util.getMapFromDir("university_change_name", "'", 0, 1);
		}
		// 提取出expertName、expertUnit、projectId、projectName、expertRole、source信息
		// 作为这一步的输出信息
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			String projectId = infos[ProjectStructure.PROJECT_ID];
			String projectName = infos[ProjectStructure.name];
			String unit = infos[ProjectStructure.unit];
			String projectRankCode = infos[ProjectStructure.rank_code_f];
			String source = "21";
			for(int i=0;i<4;i++){
				String expertName = infos[ProjectStructure.leader + i*2];
				String expertUnit = infos[ProjectStructure.leader + i*2 + 1];
				String expertRankCode = infos[ProjectStructure.first_author_rank_code + i];
				int expertRole = i+1;
				if(!expertName.equals("null")){
					if(i == 0){ //如果是第一作者
						if(expertUnit.equals("null")){ //如果一作的单位为空
							expertUnit = unit;
							expertRankCode = projectRankCode;
						}
						source = "2";
					}else { //如果是其他作者
						source = "21";
					}
					if(!expertUnit.equals("null") && (expertRankCode.charAt(2) == '1' ||expertRankCode.equals("022") || expertRankCode.equals("032"))){
						if(universityNameMap.containsKey(expertUnit)){
							System.out.print("高校变更名" + expertUnit);
							expertUnit = universityNameMap.get(expertUnit);
							System.out.print("\t" + expertUnit);
						}
						if(!(expertName.contains("课题组") || expertName.contains("编辑部") || expertName.contains("项目组") || expertName.contains("不公开姓名") || expertName.contains("发明人"))){
							expertName = expertName.replaceAll("^\\(.*?\\)|^(\\d)*\\.|^\\)|(\\d)*$", "");
							String result = expertName + "\t" + expertUnit + "\t" + projectId + "\t" + projectName + "\t" + expertRole + "\t" + source;
							System.out.println(result);
							context.write(new Text(result), NullWritable.get());
						}
					}
				}
			}
		}
	}
}
