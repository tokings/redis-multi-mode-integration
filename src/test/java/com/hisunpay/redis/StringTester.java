package com.hisunpay.redis;

import java.util.regex.Pattern;

public class StringTester {
	
	public static void main(String[] args) {
		
		Pattern pt = Pattern.compile("[\\s+]+");
		
		String src = " a ";
		String[] arr = src.split("[\\s+]+");
		System.out.println(arr.length);
		for(int i=0;i<arr.length;i++) {
			System.out.println(i + ":" + arr[i]);
		}
		
		System.out.println("===========");
		arr = pt.split(src.trim());
		System.out.println(arr.length);
		for(int i=0;i<arr.length;i++) {
			System.out.println(i + ":" + arr[i]);
		}
	}
}
