package com.autoStep.export;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;

public class MulitipleExportCorpus extends Configured implements Tool{
	/**
	 * 
	 * @author yhj
	 * 导出论文语料
	 */
	public static class MulitipleExportPaperCorpusMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		public Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[PaperStructure.Abstract].equals("null")){
				outValue.set(infos[1] +"," + infos[2]);
			}else {
				outValue.set(infos[1] +"," + infos[2] + " " + infos[PaperStructure.Abstract]);
			}
			outKey.set(infos[PaperStructure.subject_code_f] + "\t" + infos[PaperStructure.unit_type] + "\t" + infos[PaperStructure.province_code_f]);
			context.write(outKey, outValue);
		}
	}
	
	/**
	 * 
	 * @author yhj
	 * 导出专利的语料
	 */
	public static class MulitipleExportPatentCorpusMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		public Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			if(infos[PatentStructure.Abstract].equals("null")){
				outValue.set(infos[1] + "," + infos[2]);
			}else {
				outValue.set(infos[1] + "," + infos[2] + " " + infos[PatentStructure.Abstract]);
			}
			outKey.set(infos[PatentStructure.subject_code_f] + "\t" + infos[PatentStructure.unit_type] + "\t" + infos[PatentStructure.province_code_f]);
			context.write(outKey, outValue);
		}
	}
	 
	/**
	 * 
	 * @author yhj
	 * 导出项目语料
	 */
	public static class MulitipleExportProjectCorpusMap extends Mapper<Object, Text, Text, Text>{
		public Text outKey = new Text();
		public Text outValue = new Text();
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = value.toString().split("\t");
			outValue.set(infos[1] + "," + infos[2]);
			outKey.set(infos[ProjectStructure.subject_code_f] + "\t" + infos[ProjectStructure.unit_type] + "\t" + infos[ProjectStructure.province_code_f]);
			/*
			null	00162002-b115-4cf6-813e-ea39ec6607be	吡啶硫酮铜络合物抑制去泛素化酶克服多发性骨髓瘤硼替佐米耐药的机制研究	师宪平	师宪平	广州医科大学	null	null	null	null	null	null	null	广州医科大学	null	广东省基础与应用基础研究	null	null	去泛素化酶;肿瘤;耐药	deubiquitinases;cancer;drug resistance	20S蛋白酶体β5亚基的扩增与点突变是多发性骨髓瘤对靶向治疗药物硼替佐米耐药的主要原因。第二代蛋白酶体抑制剂选择作用于20S蛋白酶体，在克服硼替佐米耐药中已显示出局限性。去泛素化酶抑制剂b-AP15的研究表明其能抑制耐药细胞生长。因此，关注20S蛋白酶体上游的去泛素化酶，寻找靶点明确的去泛素化酶抑制剂将是克服硼替佐米耐药的新途径。我们前期研究证明金属络合物CuPT抑制去泛素化酶活性，诱导耐药细胞凋亡，进一步分子对接显示其能与19S蛋白酶体去泛素化酶结合。由此我们提出去泛素化酶可能是CuPT克服硼替佐米耐药的主要靶点。本项目拟采用动态酶活性检测、泛素链解离实验、去泛素化酶标签法等技术进行体内外及耐药病人半在体研究，深入探讨CuPT介导的去泛素化酶抑制与抗耐药的关系和分子机理。本项目预期结果将为CuPT靶向去泛素化酶克服硼替佐米耐药提供新思路，也为该化合物解决耐药的临床转化研究提供实验数据。	The amplification and point mutation of 20S proteasome β5 subunit is the main reason of targeted therapeutic drug bortezomib resistant in multiple myeloma and other cancers treatment. The existing second generation of proteasome inhibitors selectively inhibit the 20S proteasome and show the limitation in overcoming bortezomib resistance. The deubiquitinases inhibitor b-AP15 can inhibit the growth of bortezomib-resistant cells. Therefore, pay attention to the deubiquitinases in the upstream of 20S proteasome, finding the specific deubiquitinases inhibitor will be a new approach to overcome bortezomib resistance. Our previous studies demonstrate the metal complexes CuPT inhibits deubiquitinases activity and induces apoptosis of  bortezomib-resistant cells. Further molecular docking study shows CuPT could combine with 19S proteasomal deubiquitinases. We thus propose deubiquitinases may be the main target for CuPT in overcoming bortezomib resistance. In this project, we would use a series of technologies such as dynamic enzyme activity detection, ubiquitin chain dissociation experiment and deubiquitinases labeling techniques to proceed a study in vitro and in vivo as well as in drug resistant patients primary cells. We will discuss the relationship between deubiquitinases inhibition and drug resistance overcoming, and find the molecular mechanisms of anti-drug resistance effect mediated by CuPT. The results of this project would provide new ideas to overcome bortezomib resistance by targeting deubiquitinases, and it also provides the experimental data for CuPT in clinical translational research to overcome bortezomib resistance.	null	null	null	null	2015	null	http://www.nstrs.cn/xiangxiBG.aspx?id=35747&flag=2	6	3	344016	01	Z9	广州医科大学	01	44	00001668	011	null	011	null	null	null	fb5c1718-9522-46da-8e3d-9d4493bed134	null	null	null
			ProjectStructure.subject_code_f 33 -> Z9
			ProjectStructure.unit_type 32 -> 01
			ProjectStructure.province_code_f 36 -> 44
			* */
			System.err.println(outKey);
			System.err.println(outValue);
			System.err.println("-----------------------");
			context.write(outKey, outValue);
		}
	}
	
	
	public static class MulitipleExportCorpusReduce extends Reducer<Text, Text, Text, NullWritable>{
		private MultipleOutputs<Text,NullWritable> mos;
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] infos = key.toString().split("\t");
			String subjectCode = infos[0];
			String typeCode = infos[1];
			String province = infos[2].split(";")[0];
			String type = "other";
			if(typeCode.equals("01")){
				type = "university";
			}else if (typeCode.equals("02")) {
				type = "institue";
			}else if (type.equals("03")) {
				type = "company";
			}
			for(Text value:values){
				mos.write("outpath", value, NullWritable.get(), subjectCode + "/" + type + "/" + province);
			}
		}
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			mos.close();
		}
	}


	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2){
			System.err.println("Usage: Data Deduplication <in> <out>");
			System.exit(2);
		}
		conf.addResource("core-site.xml");
		conf.addResource("hdfs-site.xml");
		conf.addResource("mapred-site.xml");
		conf.addResource("yarn-site.xml");
		conf.set("mapred.jar", "F:\\JavaProjects\\DataOperate\\target\\DataOperate-2.0.jar");//E:\AutoDataProcess\ExportStr\MulitipleExportCorpus.jar
		Job job = Job.getInstance(conf);
		job.setJarByClass(MulitipleExportCorpus.class);
		job.setMapperClass(MulitipleExportProjectCorpusMap.class);
		job.setReducerClass(MulitipleExportCorpusReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);
		MultipleOutputs.addNamedOutput(job,"outpath",TextOutputFormat.class,Text.class,NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		return job.waitForCompletion(true)? 0:1;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MulitipleExportCorpus(), args));
	}
}
