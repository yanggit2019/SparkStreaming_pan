/**
 * OrcUtil.java
 * com.hainiuxy.mr.run.util
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.hainiuxy.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;

/**
 * orcUtil工具类， 读写orc文件
 * 应用步骤：<br/>
 * 读orc：<br/>
 * 		setOrcTypeReadSchema()<br/>
 * 		getOrcData()<br/>
 * 
 * 写orc：<br/>
 * 		setOrcTypeWriteSchema()<br/>
 * 		addAttr()<br/>
 * 		serialize()<br/>
 * 
 * @author   潘牛                      
 * @Date	 2019年6月5日 	 
 */
public class OrcUtil {
	
	/**
	 * 读取orc文件的inspector对象
	 */
	StructObjectInspector inspectorR = null;
	
	
	/**
	 * 写orc文件的inspector对象
	 */
	StructObjectInspector inspectorW = null;
	
	/**
	 * 存储一行的数据
	 */
	List<Object> realRow = null;
	
	/**
	 * orc文件序列化对象
	 */
	OrcSerde serde = null;
	
	
	/**
	 * 设置读orc的inspector对象
	*/
	public void setOrcTypeReadSchema(String schema){
		// 根据orc文件的结构，获取对应的typeinfo对象
		TypeInfo typeinfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
		// 通过typeinfo对象获取具体的inspector对象
		inspectorR = (StructObjectInspector) OrcStruct.createObjectInspector(typeinfo);
	}
	
	/**
	 * 设置写orc的inspector对象
	*/
	public void setOrcTypeWriteSchema(String schema){
		// 根据orc文件的结构，获取对应的typeinfo对象
		TypeInfo typeinfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
		// 根据typeinfo 获取写orc文件的inspector对象
		inspectorW = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeinfo);
	}
	
	/**
	 * 获取指定orc格式文件字段的值
	 * @param orcSturct orc文件数据
	 * @param filedName 字段名称
	 * @return 指定字段的值
	*/
	public String getOrcData(OrcStruct orcSturct, String filedName){
		// 根据字段名称，获取对应的 StructField对象
		StructField fieldRef = inspectorR.getStructFieldRef(filedName);
		// 通过 对应的 StructField对象，从orcData 里面，取出 对应字段的值
		Object obj = inspectorR.getStructFieldData(orcSturct, fieldRef);
		String filedData = null;
		if (obj != null) {
			filedData = String.valueOf(obj);

			filedData = "".equals(filedData) || "null".equals(filedData) ? null : filedData;
		}

		return filedData;
	}
	
	
	/**
	 * 写orc时，添加要写入orc文件的字段可变数组
	 * @param objs 可变数组
	 * @return 
	*/
	public OrcUtil addAttr(Object... objs){
		if(realRow == null){
			realRow = new ArrayList<Object>();
		}
		
		for(Object obj : objs){
			realRow.add(obj);
		}
		
		return this;
	}

	/**
	 *  将 这一行的数据 序列化成 orc文件格式
	*/
	public Writable serialize() {
		// 每次new新的
		serde = new OrcSerde();
		Writable w = serde.serialize(realRow, inspectorW);
		// 序列化后重新创建接收数据的列表对象
		realRow = new ArrayList<Object>();

		return w;
		
	}
	
}

