package com.hisunpay.redis.single;


/**
 * 
 * 主键序号生成器接口 <p>
 * 支持两种序号生成方式: <p>
 * 1. 整型自增：1,2,3,4,... <br/>
 * 2. 按时间范围自增，如格式为 yyyyMMdd，则生成序号为: 20141024001,20141024002...20141025001
 * <p>
 * Examples:
 * <blockquote>
 * <pre>
 * // 返回 TEST 的自增序号，整型递增 
 * EAP.keyTool.getInteger("TEST");
 * // 返回 TEST 的自增序号，按日递增，eg: 2014091200004, 后5位为序号
 * EAP.keyTool.getString("TEST", PrimaryKey.format.yyyyMMdd, 5);
 * </pre>
 * </blockquote>
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月22日 下午4:16:14
 *
 */
public interface IKeyTool
{

	/**
	 * 获取表主键序号
	 * @param table 表名
	 * @return 序号
	 */
	public long getInteger(String table);
	
	/**
	 * 获取表格式化的主键序号
	 * @param table 表名
	 * @param format 格式
	 * @param seqLength 序号长度
	 * @return 序号
	 * @throws Exception 
	 */
	public String getString(String table, PrimaryKey.format format, int seqLength) throws Exception;
	
}
