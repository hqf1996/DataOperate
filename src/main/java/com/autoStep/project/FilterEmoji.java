package com.autoStep.project;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.structure.ProjectStructure;

/**
 * 过滤掉Emoji的字符串
 * 由于mysql默认编码为utf-8，占3个字节，一些表情或者非常见字符，比如该例子中“\xF0\x9D\x94\xB9”占4个字节，插入失败。
 * @author yhj
 *
 */
public class FilterEmoji {
	public static class FilterEmojiMap extends Mapper<Object, Text, Text, NullWritable>{
		private Text outKey = new Text();
		public String filterEmoji(String source,String slipStr) {  
			if(StringUtils.isNotBlank(source)){
				return source.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", slipStr);
			}else{
				return source;
			}
		}
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			infos[ProjectStructure.abstract_ch] = filterEmoji(infos[ProjectStructure.abstract_ch], "");
			infos[ProjectStructure.abstract_en] = filterEmoji(infos[ProjectStructure.abstract_en], "");
			outKey.set(StringUtils.join(infos, "\t"));
			context.write(outKey, NullWritable.get());
		}
	}
}
