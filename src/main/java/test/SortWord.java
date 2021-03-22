package test;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class SortWord {
	public static class SortWordMap extends Mapper<Object, Text, MyKey, Text>{
		MyKey sum = new MyKey();
		Text ip = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, MyKey, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			sum.set(Integer.valueOf(infos[1]));
			ip.set(infos[0]);
			context.write(sum, ip);
		}
	}
	
	public static class SortWordReduec extends Reducer<MyKey, Text, Text, MyKey>{
		@Override
		protected void reduce(MyKey key, Iterable<Text> values,
				Reducer<MyKey, Text, Text, MyKey>.Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			for(Text value:values){
				context.write(value, key);
			}
		}
	}
	
	public static class MyKey extends IntWritable {
		public MyKey() {
			// TODO Auto-generated constructor stub
			super();
		}
		public MyKey(int value) {
			super(value);
		}
		@Override
		public int compareTo(IntWritable o) {
			// TODO Auto-generated method stub
			int thisValue = this.get();  
		    int thatValue = ((IntWritable)o).get();  
		    return (thisValue<thatValue ? 1 : (thisValue==thatValue ? 0 : -1));
		}
    }
	
	
	
}
