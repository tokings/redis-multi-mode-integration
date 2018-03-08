package com.hisunpay.redis.single;


/**
 * 主键序号生成器接口实现类
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月22日 下午4:15:29
 *
 */
public class KeyTool implements IKeyTool
{
	/**
	 * 生成整型自增序号，不同表不同自增序号
	 * @param table 表名
	 */
	public long getInteger(String table)
	{
		return PrimaryKeyUtil.getInteger(table);
	}

	/**
	 * 生成日期格式化的序号，按日期格式指定的范围，滚动计次生成序号，序号前缀指定格式日期串<br/>
	 * @param table		表名
	 * @param format	日期格式	 
	 * @param seqLength	序号长度，不足补0
	 * @return 序号
	 * @see PrimaryKey
	 * @throws Exception
	 */
	public String getString(String table, PrimaryKey.format format, int seqLength) throws Exception
	{
		return PrimaryKeyUtil.getString(table, format, seqLength);
	}
	
}
