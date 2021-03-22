package com.zhanglb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;

public class HeapSort {
	private static Comparator cp;
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<String ,Integer> map = new HashMap<String, Integer>();
		map.put("A", 1);
		map.put("B", 3);
		map.put("C", 5);
		map.put("D", 2);
		map.put("E", 1);
		map.put("F", 19);
		map.put("G", 10);
		System.out.println(map);
		HeapSort heap =new HeapSort();
		map = heap.sortMap(map, 10);
		System.out.println(map);
	}
	
    /**
     * 鐏忓搮ap娴状櫦alue阎ㄥ嫬锟介棿绮犳径褍鍩岀亸蹇曟暏閸棙甯挞崚锟?楠炶泛褰ф潻鏂挎礀閸揿硿ount娑掳拷
     * 
     * @param oldMap 鐟曚焦甯撴惔蹇曟畱Map
     * @param count 鏉╂柨娲栫抛鏉跨秿閺侊拷
     * @return 阉烘帒銈芥惔蹇曟畱Map鐎电钖?
     * @throws 
     */
    public Map sortMap(Map oldMap,int count) {  
        ArrayList<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(oldMap.entrySet());  
        heapSort(list, count, new Comparator<Map.Entry<String, Integer>>() {  
            @Override  
            public int compare(Entry<java.lang.String, Integer> arg0,  
                    Entry<java.lang.String, Integer> arg1) {  
                return arg0.getValue() - arg1.getValue();  
            }  
        });  
        Map newMap = new LinkedHashMap();  
        for (int i = 0; i < list.size() && i<count; i++) {  
            newMap.put(list.get(i).getKey(), list.get(i).getValue());  
        }  
        return newMap;  
    }
	
    
    public Map sortMap(Map oldMap){
    	return this.sortMap(oldMap, oldMap.size());
    }
    
    /**
     * 濞夋稑鐎风€圭偟骞囬崼鍡桦笓鎼村骏绱濋幒鎺戙偨閸揿硿ount娑掳拷
     * 
     * @param count 镨佹澘缍嶉弫锟?
     * @return 
     * @throws 
     */
	public static<T> void heapSort(List<T> list,int count,Comparator<T> cp){
		HeapSort.cp=cp;
		T[] ls=(T[]) list.toArray();
		HeapList hList=new HeapList(ls);
		buildMaxHeap(hList);
		int cou= count;
		for(int i=hList.heapSize-1;i>=1&&cou>0;i--,cou--){
			Object temp= hList.arr[0];
			hList.arr[0]=hList.arr[i];
			hList.arr[i]=temp;
			hList.heapSize--;
			maxHeapify(hList, 0);
		}
		cou= count;
		ListIterator<T> i = list.listIterator();
                for (int j=ls.length-1; j>=0&&cou>0; j--,cou--) {
                i.next();
                i.set(ls[j]);
                }
	}
	public static void buildMaxHeap(HeapList a){
		for(int i=a.heapSize>>1-1;i>=0;i--){//>>1鐞涖劎銇氶梽銈勪簰2
			maxHeapify(a, i);
		}
	}
	public static void maxHeapify(HeapList a,int i){
		int l=2*i+1;//left leaf
		int r=2*i+2;//right leaf
		int largest;
		if(l<a.heapSize&&HeapSort.cp.compare(a.arr[l],a.arr[i])>0){
			largest=l;
		}else{
			largest=i;
		}
		if(r<a.heapSize&&HeapSort.cp.compare(a.arr[r],a.arr[largest])>0){
			largest=r;
		}
		if(largest!=i){
			Object temp=a.arr[i];
			a.arr[i]=a.arr[largest];
			a.arr[largest]=temp;
			maxHeapify(a,largest);
		}
	}
	private static class HeapList<T>{
		public T[] arr;
		public int heapSize;
		public HeapList(T[] arr){
			this.arr= arr;
			this.heapSize=arr.length;
		}
	}

}
