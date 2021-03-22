package test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import com.structure.MapTable;
import com.unitProcess.UnitCleaner;

public class TempTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String unit = "sfa  asf~·`!@#$%^&*_+=?./~·！@#￥/%……-？。";
//		String regEx="[\\s*`~!@#$%^&*+=|{}':'\\[\\].<>?~·！@#￥%……&*_\\-——+|{}【】'；：”“’。，？]";
//		Pattern pattern = Pattern.compile(regEx);
//		unit = pattern.matcher(unit).replaceAll("");
//		System.out.println(unit);
		
//		Pattern pattern = Pattern.compile("(?<=（).*(?=）)");
//		String aString = "dfaf（df（sdad）a）adfa";
//		Matcher matcher;
//		if((matcher = pattern.matcher(aString)).find()){
//			System.out.println("aaa");
//			System.out.println(matcher.group());
//		}
		
		
//		String[] infos = new String[]{"23", "qwe", "da", "ui", "eqrewv"};
//		String[] copy = Arrays.copyOf(infos, 2);
//		for(String a : copy){
//			System.out.println(a);
//		}
		
//		String unit = "⒈济源市气象局";
//		String regex = "^([（）\\d\\s１２３４５６７８９０]*(\\.|．)([\\d１２３４５６７８９０]{6})*)|^(([\\d１２３４５６７８９０]{6})*)";
//		Pattern pattern = Pattern.compile(regex);
//		Matcher matcher = pattern.matcher(unit);
//		unit = matcher.replaceAll("");
//		System.out.println(unit.replaceAll("(?<=[\\u4E00-\\u9FBF])\\s+(?=[\\u4E00-\\u9FBF])", ""));
//		
//		String unit = "山东黄金疾控中心医院打算";
//		Pattern companyPattern = Pattern.compile(".*?(医院|疾病预防控制中心|疾控中心|血液中心|妇幼保健院)");
//		Matcher matcher;
//		if ((matcher = companyPattern.matcher(unit)).find()){
//			System.out.println(matcher.group().replace("疾控中心", "疾病预防控制中心").split(",|;")[0]);
//		}
		
//		String unit = "李盛	江(汉)大学";
//		System.out.println(unit.replaceAll("^\\(.*?\\)|^(\\d)*\\.|^\\)", ""));
	
//		String unit = "AGH科技大学铸造工程技术系\tAGH科技大xue\t01\t99\t99999999\t012\tnull";
//		String[] infos = unit.split("\t");
//		System.out.println(StringUtils.join(Arrays.copyOfRange(infos, 1, infos.length), "\t"));
//		String unit = "（１２３４（）５６７８９０ｑｗｅｒｔｙｕｉｏｐａｓｄｆｇｈｊｋｌｚｘｃｖｂｎｍ，．；＇［］｛｝（）＿＋阿德法哈德发大学机械工程去全文下载委员华都是分割南开";
		
		
//		String unit = "周大生.萨达。  斯达康打sd卡";
//		String regEx="[*`~@#$%^*+=|{}\"':'\\[\\]<>?~·！@#￥%……*_\\-——+|{}＜＞【】'；：”“’，？]|（）";
//		Pattern pattern = Pattern.compile(regEx);
//		if(unit.charAt(1) != '省'){
//			System.out.println(unit.replaceAll("(?<=[\\u4E00-\\u9FBF])[\\s|.|。|]+(?=[\\u4E00-\\u9FBF])", ""));
//		}else {
//			System.out.println("aaaaaaaa");
//		}
		
//		String aString = "d)afj2";
//		System.out.println(aString.replaceAll("\\d$", "").replaceAll("^\\)", ""));
		
//		String a = "CN201620948865.5	2017-04-19	授权	权	";
//		String[] infos = a.split("\t");
//		String pattern = "^\\(.*?\\)|^(\\d)*\\.|^\\d|^\\?";
//		Pattern r = Pattern.compile(pattern);
//		Matcher m = r.matcher("?.宇杰");
//		if(!m.find()){
//			System.out.println("hhhh");
//		}else {
//			System.out.println("qqqq");
//		}
		
		String a = ")吴永生11";
		System.out.println(a.replaceAll("^\\(.*?\\)|^(\\d)*\\.|^\\)|(\\d)*$", ""));
	}

/*	public static String full2Half(String string) {  
        if (string.isEmpty()) {  
            return string;  
        }  
          
        char[] charArray = string.toCharArray();  
        for (int i = 0; i < charArray.length; i++) {  
            if (charArray[i] == 12288) {  
                charArray[i] =' ';  
            } else if (charArray[i] >= '\uFF00' &&  
                    charArray[i]  <= '\uFF5F') {  
                charArray[i] = (char) (charArray[i] - 65248);  
            }
        }  
  
  
        return new String(charArray);  
    }  
*/	
}
