package com.zhanglb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.util.Heap;

/**
 * @see 得到专家找人的前几位
 * @author yhj
 *
 */
public class GetTopExpertField {
	public static class GetTopExpertFieldMap extends Mapper<Object, Text, Text, Text>{
		Text outKey = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			outKey.set(infos[13]);//将领域作为key
			context.write(outKey, value);
		}
	}
	
	public static class GetTopExpertFieldReduce extends Reducer<Text, Text, Text, NullWritable>{
		Text outKey = new Text();
		static final int TOPNUM = 200;
		class MyEntry<K, V> implements Map.Entry<K, V>{
			K Key;
			V value;
			public MyEntry(K key, V value) {
				// TODO Auto-generated constructor stub
				this.Key = key;
				this.value = value;
			}
			@Override
			public K getKey() {
				// TODO Auto-generated method stub
				return Key;
			}

			@Override
			public V getValue() {
				// TODO Auto-generated method stub
				return value;
			}

			@Override
			public V setValue(V value) {
				// TODO Auto-generated method stub
				V oldValue = this.value;
				this.value = value;
				return oldValue;
			}
			
		}
		@SuppressWarnings("unchecked")
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			List<MyEntry<Integer, String>> list = new ArrayList<MyEntry<Integer, String>>();
			for(Text value:values){
				list.add(new MyEntry<Integer, String>(Integer.parseInt(value.toString().split("\t")[17]), value.toString()));
			}
			Heap<MyEntry<Integer, String>> heap = new Heap<MyEntry<Integer, String>>(list, new Comparator<MyEntry<Integer, String>>() {

				@Override
				public int compare(MyEntry<Integer, String> o1, MyEntry<Integer, String> o2) {
					// TODO Auto-generated method stub
					return o1.getKey() - o2.getKey();
				}
			});
			for(Object obj:heap.getTop(TOPNUM)){
				outKey.set(((MyEntry<Integer, String>)obj).getValue());
				context.write(outKey, NullWritable.get());
			}
 		}
	}
}
