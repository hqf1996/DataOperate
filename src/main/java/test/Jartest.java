package test;

import org.jsoup.Jsoup;

public class Jartest {
	public static void main(String[] args) {
		String a = "hello";
		String b = "world";
		String c = a + b;
		String d = "helloworld";
		String e = "hello";
		String f = "hello" + "world";
		System.out.println(a == "hello");//ture
		System.out.println(c == d);//false
		System.out.println(d == "helloworld");//true
		System.out.println(a == e);//true
		System.out.println(d == f);//true;
	}
}
