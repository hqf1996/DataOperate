package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ParseTest {
	public static void main(String[] args) throws IOException {
		File file = new File("C:\\Users\\hp\\Desktop\\国家杰出青年_html（结题）.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "utf-8"));
		String line = null;
		while((line = reader.readLine()) != null){
//			System.out.print(line.split("\t")[1] + "\t");
			String html = line.split("\t")[2];
//			System.out.println(html);
			parse(html);
		}
	}
	
	public static void parse(String html){
		Document document = Jsoup.parse(html);
		Elements elements = document.getElementsByClass("zyao");
		if(checkElements(elements)){
			for(Element element:elements){
				String abstractType = getInfo(element.getElementsByClass("left"));
				String abstractInfo = getInfo(element.getElementsByClass("jben"));
				if(abstractType != null && abstractInfo != null){
					System.out.println(abstractType + "\t" + abstractInfo);
				}
				String keyWordsType = getInfo(element.getElementsByTag("b"));
				String keyWords = getInfo(element.getElementsByClass("xmu"));
				if(keyWordsType != null && keyWords != null){
					System.out.println(keyWordsType + "\t" + keyWords);
				}
			}
		}
		elements = document.getElementsByClass("jben");
		if(checkElements(elements)){
			for(Element element:elements){
				String type = getInfo(element.getElementsByTag("b"));
				if(type != null && type.equals("项目负责人")){
					String name = getInfo(element.getElementsByTag("a"));
					if(name != null){
						System.out.println(name);
					} 
				}
			}
		}
	}
	
	public static boolean checkElements(Elements elements){
		if(elements != null && elements.size() > 0){
			return true;
		}
		return false;
	}
	
	public static String getInfo(Elements elements){
		return elements.size() > 0 ?elements.get(0).ownText():null;
	}
}
