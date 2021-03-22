package com.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author yhj
 * @see 堆，支持堆排序，在初始化话时会自动建立一个堆，是大顶堆还是小顶堆主要看自定义的比较器
 * @param <T> 
 */
public class Heap<T> {
	Object[] arr;
	Comparator<T> comparator;
	int sortIndex;//记录已经排序过的元素，避免重复排序
	public Heap(Object[] arr, Comparator<T> comparator) {
		this.arr =  Arrays.copyOf(arr, arr.length, Object[].class);
		this.comparator = comparator;
		sortIndex = arr.length;
		buildHeap(); //形成一个堆
	}
	
	public Heap(Collection<? extends T> c, Comparator<T> comparator) {
		// TODO Auto-generated constructor stub
		 arr = c.toArray();
		 this.comparator = comparator;
	     sortIndex = arr.length;
	     if (arr.getClass() != Object[].class)
	    	 arr = Arrays.copyOf(arr, arr.length, Object[].class);
	     buildHeap();
	}
	
	public void sort(){
		for(int i = sortIndex-1;i>0;i--){
			Object tmp = arr[i];
			arr[i] = arr[0];
			arr[0] = tmp;
			heapAdjust(0, i);
		}
		sortIndex = 0;
	}
	
	private void buildHeap(){
		for(int i = arr.length/2-1;i>=0;i--){
			heapAdjust(i, arr.length);
		}
	}
	
	/**
	 * 堆的调整
	 * @param s
	 * @param m
	 */
	@SuppressWarnings("unchecked")
	private void heapAdjust(int s,int m){
		Object target = arr[s];
		for(int j = 2*s+1;j<m; j = 2*j+1){
			if(j < m-1 && comparator.compare((T)arr[j], (T)arr[j+1]) < 0){
				j++;
			}
			if(comparator.compare((T)arr[j], (T)target) < 0){
				break;
			}
			arr[s] = arr[j];
			s = j;
		}
		arr[s] = target;
	}
	
	@SuppressWarnings("unchecked")
	public T getTop(){
		if(sortIndex < arr.length){
			return (T) arr[arr.length -1];
		}else {
			Object tmp = arr[arr.length - 1];
			arr[arr.length -1] = arr[0];
			arr[0] = tmp;
			heapAdjust(0, arr.length-1);
			sortIndex = arr.length - 1;
			return (T) arr[sortIndex];
		}
	}
	
	public Object[] getTop(int s){
		Object[] origin = new Object[s];
		int j = 0;
		for(int i = arr.length-1;i>=0 && s > 0;i--,s--){
			if(i < sortIndex && i != 0){
				Object tmp = arr[i];
				arr[i] = arr[0];
				arr[0] = tmp;
				heapAdjust(0, i);
			}
			origin[j++] = arr[i];
		}
		return Arrays.copyOf(origin, j);
	}
	
	@SuppressWarnings("unchecked")
	public T[] getTop(T[] result, int s){
		Object[] origin = new Object[s];
		int j = 0;
		for(int i = arr.length-1;i>=0 && s > 0;i--,s--){
			if(i < sortIndex && i != 0){
				Object tmp = arr[i];
				arr[i] = arr[0];
				arr[0] = tmp;
				heapAdjust(0, i);
			}
			origin[j++] = arr[i];
		}
		return (T[]) Arrays.copyOf(origin, j, result.getClass());
	}
	
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		Map<String ,Integer> map = new HashMap<String, Integer>();
		map.put("A", 1);
		map.put("B", 3);
		map.put("C", 5);
		map.put("D", 2);
		map.put("E", 1);
		map.put("F", 19);
		map.put("G", 10);
		System.out.println(map);
		Heap<Map.Entry<String, Integer>> heap = new Heap<Map.Entry<String, Integer>>(map.entrySet(), new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				// TODO Auto-generated method stub
				return o1.getValue() - o2.getValue();
			}
			
		});
		heap.sort();
		System.out.println(heap.getTop());
		for(Object each : heap.getTop(100)){
			System.out.println((Map.Entry<String, Integer>)each);
		}
		for(Object each:heap.arr){
			System.out.println((Map.Entry<String, Integer>)each);
		}
	}
}
