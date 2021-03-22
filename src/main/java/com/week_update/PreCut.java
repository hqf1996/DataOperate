package com.week_update;

import com.util.Util;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 添加一些包含在merge_words中的关键词
 * @author yhj
 *
 */
public class PreCut {
	public static class PreCutMap extends Mapper<Object, Text, Text, NullWritable>{
		List<String[]> mergeWords = new ArrayList<String[]>();
		Text outKey = new Text();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mergeWords = Util.getListFromDir("merge_words", "\t", 1); //  /user/mysqlOut/merge_words
			System.out.println(mergeWords.size());
		}

		@Override
		protected void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] infos = line.split(" ");
			Set<String> addWords = new HashSet<String>();//存储增加的词根
			//对每个分词判断是否前缀匹配词根
			for(int i = 1; i < infos.length;i++){
				for(String[] arr: mergeWords){
					if(infos[i].startsWith(arr[0])){
						//如果包含，加入该词根
//						System.out.println(line);
						addWords.add(arr[0]);
						break;
					}
				}
			}
			if(addWords.size() > 0){
				outKey.set(line + " " + StringUtils.join(addWords.toArray(), " "));
			}else {
				outKey.set(line);
			}
			context.write(outKey, NullWritable.get());
		}
	}
}
