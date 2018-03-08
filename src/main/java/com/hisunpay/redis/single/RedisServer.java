package com.hisunpay.redis.single;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

/**
 * Redis Server
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月22日 下午4:16:47
 *
 */
class RedisServer 
{

	private String redisServerIp = Protocol.DEFAULT_HOST;
	
	private int redisServerPort = Protocol.DEFAULT_PORT;

	private JedisPool jedisPool;
	
	private Jedis pingJedis;
	
	private GenericObjectPoolConfig config = new GenericObjectPoolConfig();
	
	private boolean running = false;
	
	private int timeout = 10000;
	
	private final static RedisServer server = new RedisServer();

	
	public  static RedisServer getInstance()
	{
		return server;
	}
	
	/**
	 * 从连接池获取连接
	 * @return jedis
	 */
	public synchronized Jedis getResource()
	{
		return jedisPool.getResource();
	}

	/**
	 * 回收连接
	 * @param jedis 待回收的连接
	 */
	public void returnResource(Jedis jedis)
	{
		if (jedis != null) {
			jedis.close();
		}
	}

	/**
	 * 连接 redis server
	 * @param ip server ip
	 */
	public void start(String ip)
	{
		if(jedisPool != null && pingJedis.isConnected() && running) return;
		
		server.redisServerIp = ip;
				
		config.setBlockWhenExhausted(true);
		config.setMaxTotal(200); 	// 可分配实例数
		config.setMaxIdle(200);
		config.setMaxWaitMillis(10000);		// 当分配实例时，最大的等待时间
		
		connect();
	}
	
	/**
	 * 根据IP、端口连接 redis server
	 * @param ip server ip
	 */
	public void start(String ip,int port)
	{
		if(running && jedisPool != null && pingJedis.isConnected()) return;
		
		server.redisServerIp = ip;
		server.redisServerPort = port;

		config.setBlockWhenExhausted(true);
		config.setMaxTotal(200); 	// 可分配实例数
		config.setMaxIdle(200);
		config.setMaxWaitMillis(10000);		// 当分配实例时，最大的等待时间
		
		connect();
	}

	public void connect()
	{
		jedisPool = new JedisPool(config, redisServerIp, redisServerPort, timeout);
		pingJedis = jedisPool.getResource();
		running = true;
	}
	
	/**
	 * 停止服务，销毁连接池
	 */
	public void stop()
	{
		if(running && pingJedis != null && pingJedis.isConnected()) {
			pingJedis.disconnect();
			pingJedis = null;
		}
		
		if(running && jedisPool != null) {
			jedisPool.destroy();
			jedisPool = null;
		}
	}
	
	public boolean isRunning() {
		return running;
	}

	/**
	 * Ping
	 * @return 返回 false 说明断开了连接 
	 */
	public boolean ping()
	{
		try {
			String ret = pingJedis.ping();
			
			if ("PONG".equals(ret)) {
				return true;
			}
		} catch (Exception e) {

		}
		
		return false;
	}

}
