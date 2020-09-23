/**
 * Utils.java
 * com.hainiuxy.mrrun.util
 * Copyright (c) 2018, 海牛版权所有.
 * @author   潘牛                      
*/

package com.hainiuxy.util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

/**
 * 
 * @author   潘牛                      
 * @Date	 2018年12月14日 	 
 */
public class Utils {
	
	/**
	 * 对String，数组，map，Collection 判空
	 * @param o
	 * @return true：空；false:not 空
	*/
	public static boolean isEmpty(Object o){
		if(o == null){
			return true;
			
		}else if(o.getClass() == String.class){
			return String.valueOf(o).trim().equals("");
			
		}else if(o instanceof Map){
			return ((Map<?,?>)o).isEmpty();
			
		}else if(o instanceof Collection){
			return ((Collection<?>)o).isEmpty();
			
		}else if(o instanceof Object[]){
			return Array.getLength(o) == 0;
			
		}
		return false;
		
	}
	
	public static boolean isNotEmpty(Object o){
		return !isEmpty(o);
	}
	
	public static void main(String[] args) {
		System.out.println(Utils.isEmpty(new String[]{}));
	}

}

