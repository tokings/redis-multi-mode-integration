package com.hisunpay.redis.single;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import redis.clients.jedis.Jedis;

/**
 * 基于 REDIS 的主键生成器，使用之前需启动 RedisServer
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月22日 下午4:13:21
 */
public class PrimaryKeyUtil 
{
			
	private static SimpleDateFormat sdfy = new SimpleDateFormat("yyyy");
	
	private static SimpleDateFormat sdfym = new SimpleDateFormat("yyyyMM");
	
	private static SimpleDateFormat sdfymd = new SimpleDateFormat("yyyyMMdd");
	
	private static SimpleDateFormat sdfsymd = new SimpleDateFormat("yyMMdd");
	
	
	/**
	 * 生成整型自增序号，不同表不同自增序号
	 * @param table	表名
	 * @return
	 */
	public static long getInteger(String table)
	{
		Jedis jedis = null;
		long seq = 0;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			seq = jedis.incr(table);
			return seq;
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 生成日期格式化的序号，按日期格式指定的范围，滚动计次生成序号，序号前缀指定格式日期串。<br/>
	 * @param table		表名
	 * @param format	日期格式	 
	 * @param seqLength	序号长度，不足补0
	 * @return
	 * @see PrimaryKey
	 * @throws Exception
	 */
	public static String getString(String table, PrimaryKey.format format, int seqLength) throws Exception
	{				
		Date current = Calendar.getInstance().getTime();
		String date = "";
		
		switch(format) {
			case yyyy:
				date = sdfy.format(current);
				break;
			case yyyyMM:
				date = sdfym.format(current);
				break;
			case yyyyMMdd:
				date = sdfymd.format(current);
				break;
			case yyMMdd:
				date = sdfsymd.format(current);
				break;
		}
				
		table = table  + ":" + date;
		
		Jedis jedis = null;
		long seq = 0;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			seq = jedis.incr(table.toString());
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
		
		return date + String.format("%0" + seqLength + "d", seq);
	}
	
}
