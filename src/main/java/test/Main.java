package test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;


public class Main {
//	public static void main(String[] args) {
//		String unit = "null";
//		String firstOrganization = "河北省中药研究与开发重点实验室,承德医学院";
//		unit = firstOrganization.replaceAll("\\(.*?\\)", "").replaceAll("\\（.*?\\）", ""); 
//		Pattern pattern = Pattern.compile("[^\\d\\s()（）、,，.].*$");  //去掉前缀是数字空格括号的
//		Matcher m = pattern.matcher(unit);
//		System.out.println(firstOrganization);
//		if(!unit.equals("null")&&m.find()){  //m.find()必须要检验，否则会报错
//			unit = m.group(0);
//			unit = unit.split("!|;|/|,|，|、")[0];
//		}
//		System.out.println(unit);
//		System.out.println(firstOrganization);
		
//		String aa = "邓云峰( );李竞(中国安全生产科学研究院);裴葆青(北京航空航天大学);卜春光(中科院沈阳自动化研究所);郭再富(中国安全生产科学研究院);";
//		System.out.println(aa.replaceAll("\\(.*?\\)", ""));
//		String[] authors = aa.split(";");
//		for(int i = 0;i<authors.length && i<4;i++){
//			Pattern pattern = Pattern.compile("(.*?)\\((.*?)\\)");
//			Matcher matcher = pattern.matcher(authors[i]);
////			System.out.println(matcher.groupCount());
//			if(matcher.find() && matcher.groupCount()>=2){
//				System.out.println(matcher.group(1));
//				System.out.println(matcher.group(2));
//			}else {
//				System.out.println(authors[i]);
//			}
//		}
		
//		String a = "null  null  \"Sandwich\"型金催化剂的制备、结构和催化稳定性能  祁彩霞;  祁彩霞  烟台大学  null  null  null  null  null  null  烟台大学  null  面上项目  null  null  Sandwich-type gold catalyst；CO oxidation；catalytic stability；structure-effect relationship；  null  项目的核心内容是突破传统负载型金属催化剂的制备思路，采用初湿浸渍法和嫁接法等合成技术在原已优化的氧化物负载的纳米金催化剂上\"反向\"沉积硅、铝氧化物或含Si、N、P、S链长不同的烷基基团制备具有类似夹心面包(sandwich)结构的新型金催化剂,试图在控制金微粒积聚的同时，减少或根除引起催化剂失活的因素，甚至增加催化剂过滤、转化毒物分子的功能。达到在反应活性可接受的前提下, 提高金催化剂在直接暴露空气中的长期存放或长期低温CO氧化反应或燃料电池重整反应气氛下CO选择氧化反应的稳定性。其科学意义在于，基于金属-载体相互作用的理论基础，扩大金微粒与载体及其包覆物的接触面积，或用有机基团调变催化剂的物化性能,探索\"sandwich\"型金催化剂的创新制备方法、成型催化剂组成、形貌和物理化学结构与其催化性能,特别是稳定性的关系,发展金催化理论,为制备催化活性好、稳定性更高的金催化剂奠定实验和理论基础。  夹心型金催化剂；CO氧化反应；催化稳定性；构效关系；  null  null  null  null  2010  37  http://www.nstrs.cn/xiangxiBG.aspx?id=55952&flag=1  6  1  101  01  Z9  烟台大学  01  37  00524693  011  烟台市";
//		String[] result = a.split("  ");
//		System.out.println(result.length);
//		System.out.println(result[ProjectStructure.keywords_ch]);
//		if(result[ProjectStructure.keywords_en].equals("null") && !result[ProjectStructure.abstract_en].equals("null")){
//			char firstChar = result[ProjectStructure.abstract_en].charAt(0);
//			if((firstChar>'z' ||  firstChar<'a') && (firstChar>'Z' ||  firstChar<'A')){
//				if(result[ProjectStructure.abstract_en].indexOf(";") != -1 || result[ProjectStructure.abstract_en].indexOf("；") != -1){
//					result[ProjectStructure.keywords_en] = result[ProjectStructure.keywords_ch];
//					result[ProjectStructure.keywords_ch] = result[ProjectStructure.abstract_en];
//					result[ProjectStructure.abstract_en] = "null";
//					System.out.println("success");
//					System.out.println(result[ProjectStructure.keywords_ch]);
//				}else {
//					System.out.println("fail");
//				}
//			}else {
//				System.out.println("fail");
//			}
//		}else {
//			System.out.println("aaaa");
//		}
//	}
	
    public static void main(String[] args) {
//        String[] strArr = new String[] { "www.micmiu.com", "!@#$%^&*()_+{}[]|\"'?/:;<>,.", "！￥……（）——：；“”‘’《》，。？、", "不要;啊", "やめて", "韩佳人", "???" };
//        for (String str : strArr) {
//            System.out.println("===========> 测试字符串：" + str);
//            System.out.println("正则判断结果：" + isChineseByREG(str) + " -- " + isChineseByName(str));
//            System.out.println("Unicode判断结果 ：" + isChinese(str));
//            System.out.println("详细判断列表：");
//            char[] ch = str.toCharArray();
//            for (int i = 0; i < ch.length; i++) {
//                char c = ch[i];
//                System.out.println(c + " --> " + (isChinese(c) ? "是" : "否"));
//            }
//        }
//    	String unit = ",";
//    	unit = unit.replaceAll("[\\s.]", "");
//    	String[] unitFirst = unit.split("！|!|;|/|、|,");
//    	
//    	System.out.println(unitFirst.length);
    	Set<String> set = new HashSet<>();
    	set.add("a");
    	set.add("b");
    	System.out.println(StringUtils.join(set.toArray(), " "));
    }
 
    
    public static boolean isEmoij(String s) {
		Pattern pattern = Pattern.compile("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]+");
        Matcher matcher = pattern.matcher(s);
        if(matcher.find()){
        	System.out.println(matcher.group());
			return true;
		}else {
			return false;
		}
	}
    
    // 根据Unicode编码完美的判断中文汉字和符号
    private static boolean isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {
            return true;
        }
        return false;
    }
 
    // 完整的判断中文汉字和符号
    public static boolean isChinese(String strName) {
        char[] ch = strName.toCharArray();
        for (int i = 0; i < ch.length; i++) {
            char c = ch[i];
            if (isChinese(c)) {
                return true;
            }
        }
        return false;
    }
 
    // 只能判断部分CJK字符（CJK统一汉字）
    public static boolean isChineseByREG(String str) {
        if (str == null) {
            return false;
        }
        Pattern pattern = Pattern.compile("[\\u4E00-\\u9FBF]+");
        return pattern.matcher(str.trim()).find();
    }
 
    // 只能判断部分CJK字符（CJK统一汉字）
    public static boolean isChineseByName(String str) {
        if (str == null) {
            return false;
        }
        // 大小写不同：\\p 表示包含，\\P 表示不包含
        // \\p{Cn} 的意思为 Unicode 中未被定义字符的编码，\\P{Cn} 就表示 Unicode中已经被定义字符的编码
        String reg = "\\p{InCJK Unified Ideographs}&&\\P{Cn}";
        Pattern pattern = Pattern.compile(reg);
        return pattern.matcher(str.trim()).find();
    }
	
}
