package com;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 修改OutputFormat
 * 将输出到文件改成csv格式
 * @author yhj
 *
 * @param <K>
 * @param <V>
 */
public class MyTextOutputFormat<K, V> extends TextOutputFormat<K, V> {
	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		// 1、根据Configuration判定是否需要压缩，若需要压缩获取压缩格式及后缀；
		// 2. 获取需要生成的文件路径，getDefaultWorkFile(job, extension)
		// 3. 根据文件生成FSDataOutputStream对象，并return new LineRecordWriter。
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator = conf.get(SEPERATOR, "\t");
		CompressionCodec codec = null;
		String extension = ".csv";
		if (isCompressed) { // 如果是压缩，则根据压缩获取扩展名
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
			extension = codec.getDefaultExtension();
		}
		// getDefaultWorkFile用来获取保存输出数据的文件名，由FileOutputFormat类实现
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);

		// 获取writer对象
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			DataOutputStream dataOut = new DataOutputStream(codec.createOutputStream(fileOut));
			return new LineRecordWriter<K, V>(dataOut, keyValueSeparator);
		}
	}
}
