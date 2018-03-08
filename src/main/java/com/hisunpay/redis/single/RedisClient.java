package com.hisunpay.redis.single;


import java.util.List;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;

/**
 * Redis Client
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月22日 下午4:16:37
 *
 */
public class RedisClient
{
	private String ip;
	private int port;
	
	public RedisClient(String ip, int port) {
		this.ip = ip;
		this.port = port;
//		init();
	}

	public RedisClient() {
	}

	public void init() {
		ip = (ip==null || "".equals(ip)) ? Protocol.DEFAULT_HOST : ip;
		port = (port <= 0) ? Protocol.DEFAULT_PORT : port;
		if(! RedisServer.getInstance().isRunning()) {
			RedisServer.getInstance().start(ip, port);
		}
	}
	
	public void close() {
		RedisServer.getInstance().stop();
	}
	
	/**
	 * 获取redis连接资源
	 * @return
	 */
	public Jedis getResource() {
		try {
			return RedisServer.getInstance().getResource();		
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			
		}
	}
	
	/**
	 * 释放redis资源
	 * @param jedis
	 */
	public void returnResource(Jedis jedis) {
		RedisServer.getInstance().returnResource(jedis);
	}

	/**
	 * 	
	 * @param key
	 * @return
	 */
	public boolean exists(String key)
	{
		Jedis jedis = null;
	
		try {
			jedis = RedisServer.getInstance().getResource();		
			return jedis.exists(key);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 设置数据过期时间
	 * @param key
	 * @param seconds 秒
	 */
	public void expire(String key, int seconds) {
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			jedis.expire(key, seconds);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 设置数据过期时间
	 * @param key
	 * @param milliseconds 毫秒
	 */
	public void expire(String key, long milliseconds) {
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			jedis.pexpire(key, milliseconds);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public String get(String key)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			return jedis.get(key);
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}

	/**
	 * 	
	 * @param key
	 * @return
	 */
	public void set(String key, String value)
	{
		set(key, value, 0);
	}
			
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public void set(String key, String value, int expiredInSeconds)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();
			Pipeline pl = jedis.pipelined();
			pl.set(key, value);
			if (expiredInSeconds > 0) {
				pl.expire(key, expiredInSeconds);
			}
			pl.syncAndReturnAll();
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List<Object> lset(List<String[]> kvPairs, int expiredInSeconds)
	{
		Jedis jedis = null;
				
		try {
			jedis = RedisServer.getInstance().getResource();
			Pipeline pl = jedis.pipelined();
			for (String[] kv: kvPairs)
				pl.setex(kv[0], expiredInSeconds, kv[1]);
			return pl.syncAndReturnAll();
		} catch (Exception e) {
			
			throw new RuntimeException(e);			
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}			
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public long lpush(String key, String value)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();
			
			return jedis.lpush(key, value);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List<Object> lpush(List<String[]> kvList, int size)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();
			
			Pipeline pl = jedis.pipelined();
			
			for(String[] kvStr:kvList){
				pl.lpush(kvStr[0], kvStr[1]);
				pl.ltrim(kvStr[0], 0, size);
			}
			
			return pl.syncAndReturnAll();
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public long lpush(String key, String value, int seconds)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();
			long lpush = jedis.lpush(key, value);
			jedis.expire(key, seconds);
			return lpush;
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List<Object> lpush(String[] key, String value)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();
			
			Pipeline pl = jedis.pipelined();
			
			for (int i=0; i<key.length; i++) {
				pl.lpush(key[i], value);
			}
			
			return pl.syncAndReturnAll();
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List<Object> lpop(String key, int number)
	{
		Jedis jedis = null;
				
		try {
			jedis = RedisServer.getInstance().getResource();
					
			Pipeline pl = jedis.pipelined();
			
			for (int i=0; i<number; i++) {
				pl.lpop(key).get();
			}
			
			return pl.syncAndReturnAll();
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);			
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}			
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public void lrem(String key, int number, String value)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();
					
			jedis.lrem(key, number, value);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}

	/**
	 * 	
	 * @param key
	 * @return
	 */
	public void lrem(String key[], int number, String value)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();
					
			Pipeline pl = jedis.pipelined();
			
			for (int i=0; i<key.length; i++) {
				pl.lrem(key[i], number, value);
			}
			
			pl.syncAndReturnAll();
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List<String> lrange(String key, int start, int end)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			
			return jedis.lrange(key, start, end);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List<Object> lrange(Set<String> keys, int start, int end)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			Pipeline pl = jedis.pipelined();
			for(String key:keys){
				pl.lrange(key, start, end);
			}
			
			return pl.syncAndReturnAll();
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}		
	}

	/**
	 * 	
	 * @param key
	 * @return
	 */
	public String lpop(String key)
	{
		Jedis jedis = null;
				
		try {
			jedis = RedisServer.getInstance().getResource();
					
			return jedis.lpop(key);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);			
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}			
	}

	/**
	 * 	
	 * @param key
	 * @return
	 */
	public String rpop(String key)
	{
		Jedis jedis = null;
				
		try {
			jedis = RedisServer.getInstance().getResource();
					
			return jedis.rpop(key);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);			
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}			
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List<Object> rpop(String key, int number)
	{
		Jedis jedis = null;
				
		try {
			jedis = RedisServer.getInstance().getResource();
					
			Pipeline pl = jedis.pipelined();
			
			for (int i=0; i<number; i++) {
				pl.rpop(key).get();
			}
			
			return pl.syncAndReturnAll();
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);			
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}			
	}

	/**
	 * 	
	 * @param key
	 * @return
	 */
	public Set<String> keys(String pattern){
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.keys(pattern);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public boolean hexists(String key, String field)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			return jedis.hexists(key, field);
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public String hget(String key, String field)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			return jedis.hget(key, field);
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public Map<String, String> hgetall(String key)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			return jedis.hgetAll(key);
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List<String> hmget(String key, String... fields)
	{
		Jedis jedis = null;
		
		try {
			jedis = RedisServer.getInstance().getResource();		
			return jedis.hmget(key, fields);
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public void hset(String key, String field, String value)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			Pipeline pl = jedis.pipelined();
			pl.hset(key, field, value);
			pl.sync();
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public void hmset(String key, Map hash)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			Pipeline pl = jedis.pipelined();
			pl.hmset(key, hash);
			pl.sync();
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}	
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public Set hkeys(String key)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.hkeys(key);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public List hvals(String key)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.hvals(key);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public long hlen(String key)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.hlen(key);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public long hdel(String key, String field)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.hdel(key, field);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 	
	 * @param key
	 * @return
	 */
	public void del(String... keys)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			jedis.del(keys);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**自增计数器
	 * 	
	 * @param key
	 * @return
	 */
	public long incr(String key)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.incr(key);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**按步长自增计数器
	 * 	
	 * @param key
	 * @return
	 */
	public long incrBy(String key,long step)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.incrBy(key, step);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**按步长自增计数器
	 * @param key
	 * @param field
	 * @param step
	 * @return
	 */
	public long hincrBy(String key,String field,long step)
	{
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.hincrBy(key, field, step);
			
		} catch (Exception e) {
			
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	public long dbsize() {
		
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			return jedis.dbSize();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 清空当前数据库所有数据
	 */
	public void flushDB() {
		
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			jedis.flushDB();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
	
	/**
	 * 清空所有数据
	 */
	public void flushAll() {
		
		Jedis jedis = null;
		try {
			jedis = RedisServer.getInstance().getResource();
			jedis.flushAll();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			RedisServer.getInstance().returnResource(jedis);
		}
	}
}
