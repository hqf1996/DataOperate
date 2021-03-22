package com.unitProcess;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;


public class Heap<T> {
	private Comparator<T> comparator;
	public T[] arr;
	@SuppressWarnings("unchecked")
	public Heap(List<T> list, Comparator<T> comparator) {
		// TODO Auto-generated constructor stub
		arr =  (T[])list.toArray();
		this.comparator = comparator;
		heapSort();
	}
	
	public void set(T element){
		arr[0] = element;
		heapAdjust(0, arr.length-1);
	}
	
	private void heapForm(){
		for(int i = (arr.length-1)/2;i>=0;i--){
			heapAdjust(i, arr.length-1);
		}
	}
	
	public void heapAdjust(int s, int m){
		T target = arr[s];
		for(int j = s*2+1;j<=m;j = j*2+1){
			if(j<m && comparator.compare(arr[j], arr[j+1]) == 1){
				j++;
			}
			if(comparator.compare(arr[j], target) == 1){
				break;
			}
			arr[s] = arr[j]; s = j;
		}
		arr[s] = target;
	}
	
	public T getFirst(){
		return arr[0];
	}
	
	public T[] getCount(int count){
		return Arrays.copyOf(arr, count);
	}
	
	public T[] getArr(){
		return arr;
	}
	
	public void heapSort(){
		heapForm();
		for(int i= arr.length-1;i>0;i--){
			T tmp = arr[i];
			arr[i] = arr[0];
			arr[0] = tmp;
			heapAdjust(0, i-1);
		}
	}
}


