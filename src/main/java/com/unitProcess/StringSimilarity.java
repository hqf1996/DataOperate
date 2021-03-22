package com.unitProcess;

import java.util.HashMap;
import java.util.Map;


public class StringSimilarity {

	/**
	 * 最大编辑距离
	 * @param source
	 * @param target
	 * @return
	 */
	public static int LevenshteinDistance(String source, String target){
		int m = source.length(),n = target.length();
		if(m == 0){return n;}
		if(n == 0){return m;}
		int[][] matrix = new int[m+ 1][n +1];
		for(int i = 0;i<=m;i++){
			matrix[i][0] = i;
		}
		for(int j = 1;j<=n;j++){
			matrix[0][j] = j;
		}
		for(int i = 1;i<=m;i++){
			for(int j = 1;j<=n;j++){
				String tmp = source.substring(i-1, i);
				if(tmp.equals(target.substring(j-1, j))){
					matrix[i][j] = Mininum(matrix[i-1][j-1], matrix[i-1][j]+1, matrix[i][j-1]+1);
				}else {
					matrix[i][j] = Mininum(matrix[i-1][j-1], matrix[i-1][j], matrix[i][j-1]) + 1;
				}
			}
		}
		return matrix[m][n];
	}
	
	/**
	 * 最大子串(不需要连续，前后相对位置对就行)
	 * @param source
	 * @param target
	 * @return
	 */
	public static int LongestCommonSubsequence(String source, String target){
		int m = source.length(),n = target.length();
		if(m == 0 || n == 0){return 0;}
		int[][] matrix = new int[m+1][n+1];
		for(int i = 0;i<=m;i++){
			matrix[i][0] = 0;
		}
		for(int j = 1;j<=n;j++){
			matrix[0][j] = 0;
		}
		for(int i = 1;i<=m;i++){
			String tmp = source.substring(i-1, i);
			for(int j = 1;j<=n;j++){
				if(tmp.equals(target.substring(j-1, j))){
					matrix[i][j] = matrix[i-1][j-1] + 1;
				}else {
					matrix[i][j] = Maxnum(matrix[i-1][j-1], matrix[i-1][j], matrix[i][j-1]);
				}
			}
		}
		return matrix[m][n];
	} 
	
	/**
	 * 余弦相似度
	 * @param source
	 * @param target
	 * @return
	 */
	public static double cosineSimilarity(String source, String target){
		if(source.length() == 0 || target.length() == 0){return 0;}
		Map<String, Integer[]> map = new HashMap<String, Integer[]>();
		for(int i=0;i<source.length();i++){
			String tmp = source.substring(i, i+1);
			if(map.containsKey(tmp)){
				map.get(tmp)[0] ++;
			}else {
				map.put(tmp, new Integer[]{1,0});
			}
		}
		for(int i=0;i<target.length();i++){
			String tmp = target.substring(i, i+1);
			if(map.containsKey(tmp)){
				map.get(tmp)[1] ++;
			}else {
				map.put(tmp, new Integer[]{0, 1});
			}
		}
		int a = 0, b = 0, c = 0;
		for(Integer[] value: map.values()){
			a += value[0] * value[1];
			b += value[0] * value[0];
			c += value[1] * value[1];
		}
		return a/(Math.sqrt((double)b)*Math.sqrt((double)c));
	}
	
	private static int Mininum(int... values){
		if(values.length == 0){
			throw new IllegalArgumentException();
		}
		int minNum = values[0];
		for(int i = 1;i<values.length;i++){
			if(minNum > values[i]){
				minNum = values[i];
			}
		}
		return minNum;
	}
	
	
	private static int Maxnum(int... values){
		if(values.length == 0){
			throw new IllegalArgumentException();
		}
		int maxNum = values[0];
		for(int i = 1;i<values.length;i++){
			if(maxNum < values[i]){
				maxNum = values[i];
			}
		}
		return maxNum;
	}
	
	
	public static void main(String[] args) {
		System.out.println(LevenshteinDistance("(武汉)中国地质大学", "中国地质大学(武汉)") + "  " + LongestCommonSubsequence("中国地质大学信息工程学院 武汉", "中国地质大学(武汉)") 
				+ "  " + cosineSimilarity("杭州星碧科技有限公司", "杭州镭星科技有限公司"));
	}
	
}
