package com.other;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.structure.PaperStructure;

public class paperAddSpecialUnit {
	public static class paperAddSpecialUnitMap extends Mapper<Object, Text, Text, Text> {
		Text keyOut = new Text();
		Text valueOut = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if (infos.length == 10) {
				keyOut.set(infos[0]);
				context.write(keyOut, value);
			}
			if (infos.length == PaperStructure.fourth_author_f + 1) {
				keyOut.set(infos[PaperStructure.PAPER_ID]);
				context.write(keyOut, value);
			}
		}
	}

	public static class paperAddSpecialUnitReduce extends Reducer<Text, Text, Text, NullWritable> {
		Text keyOut = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] changeItem = null;
			List<String[]> resultList = new ArrayList<String[]>();
			for (Text value : values) {
				String[] infos = value.toString().split("\t");
				if (infos.length == 10) {
					changeItem = infos;
				}
				if (infos.length == PaperStructure.fourth_author_f + 1) {
					resultList.add(infos);
				}
			}
			if (!resultList.isEmpty()) {
				if (changeItem != null) {
					for (String[] infos : resultList) {
						if (infos[PaperStructure.unit].equals("null")) {
							System.out.println(infos[PaperStructure.PAPER_ID] + ":" + infos[PaperStructure.unit]
									+ "---->" + changeItem[2]);
							infos[PaperStructure.first_organization] = changeItem[1];
							infos[PaperStructure.organization] = changeItem[1];
							infos[PaperStructure.unit] = changeItem[2];
							infos[PaperStructure.unit_type] = changeItem[3];
							infos[PaperStructure.unit_f] = changeItem[4];
							infos[PaperStructure.type_code_f] = changeItem[5];
							infos[PaperStructure.province_code_f] = changeItem[6];
							infos[PaperStructure.unit_code_f] = changeItem[7];
							infos[PaperStructure.rank_code_f] = changeItem[8];
							infos[PaperStructure.city_f] = changeItem[9];
							keyOut.set(StringUtils.join(Arrays.copyOf(infos, infos.length - 4), "\t"));
							context.write(keyOut, NullWritable.get());
						}
					}
				} else {
					for (String[] infos : resultList) {
						keyOut.set(StringUtils.join(Arrays.copyOf(infos, infos.length - 4), "\t"));
						context.write(keyOut, NullWritable.get());
					}
				}
			}
		}
	}
}
