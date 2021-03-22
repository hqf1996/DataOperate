package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.structure.PaperStructure;
import com.structure.PatentStructure;
import com.structure.ProjectStructure;

public class Test01 {
	public static void main(String[] args) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://10.1.13.111:8020/user/mysql"), conf);
		// Path("hdfs://10.1.13.111:8020/user/mysqlOut/paper/part-r-00000"));
		// Path("hdfs://10.1.13.111:8020/user/addData/paper/add_paper_result2017-11-08.txt"));
		InputStream in = fs.open(new Path("hdfs://10.1.13.111:8020/user/gezp/tempTable/part-r-00000"));
//		InputStream in = fs.open(new Path("hdfs://10.1.13.111:8020/user/projectTmp/PretreatmentNstrs/part-r-00000"));
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
		String line;
		int num = 0;
		while ((line = br.readLine()) != null) {
			String[] infos = line.split("\t");
//			System.out.println(infos.length);
//			System.out.println(line);
			if(infos.length <= 7){
				System.out.println(line);
				num ++;
			}
//			String[] authors = infos[6].split(";");
//			for (int i = 0; i < authors.length && i < 4; i++) {
//				if(authors[i].length() > 80){
//					System.out.println(authors[i]);
//					continue;
//				}

//				Pattern pattern = Pattern.compile("(.*?)\\((.*?)\\)");
//				Matcher matcher = pattern.matcher(authors[i]);
//				System.out.println(authors[i]);
//				if (matcher.find() && matcher.groupCount() >= 2) {
//					String firstOrganization = matcher.group(2);
//					System.out.println(firstOrganization);
//					String unit = firstOrganization.replaceAll("\\(.*?\\)", "").replaceAll("\\（.*?\\）", ""); // 去掉括号里的内容
//					Pattern pattern1 = Pattern.compile("[^\\d\\s()（）、,，.].*$"); // 去掉前缀是数字空格括号的
//					Matcher m = pattern1.matcher(unit);
//					if (!unit.equals("null") && m.find()) { // m.find()必须要检验，否则会报错
//						unit = m.group(0);
//						unit = unit.split("!|;|/")[0];
//					}
//				}
//			}
			// System.out.println(line);
			// System.out.println(infos.length);
		}
		//
		 System.out.println(num);
		br.close();
		in.close();
		fs.close();
	}
}
