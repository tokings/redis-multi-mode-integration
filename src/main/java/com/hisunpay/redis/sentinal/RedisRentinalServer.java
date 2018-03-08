package com.hisunpay.redis.sentinal;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;

/**
 * Redis Server
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月22日 下午4:16:47
 *
 */
class RedisRentinalServer 
{
	private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
	private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
	private long maxWaitMillis = GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;

	GenericObjectPoolConfig config = new GenericObjectPoolConfig();

	private Set<String> nodes;
	
	private Jedis pingJedis;
	
	private JedisSentinelPool jedisPool;
	
	private boolean running = false;
	
	private String masterName;
	
	private final static RedisRentinalServer server = new RedisRentinalServer();

	
	public  static RedisRentinalServer getInstance()
	{
		return server;
	}
	
	/**
	 * 从连接池获取连接
	 * @return jedis
	 */
	public Jedis getResource()
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
	 * 
	 * @param masterName
	 * @param servers
	 * @param maxTotal
	 * @param maxIdle
	 * @param minIdle
	 * @param maxWaitMillis
	 */
	public void start(String masterName, String servers, int maxTotal, int maxIdle, int minIdle, long maxWaitMillis)
	{
		if(jedisPool != null && pingJedis.isConnected() && running) return;
		
		this.masterName = masterName;

		this.maxTotal = maxTotal;
		this.maxIdle = maxIdle;
		this.minIdle = minIdle;
		this.maxWaitMillis = maxWaitMillis;
		
		config.setBlockWhenExhausted(true);
		config.setMaxTotal(this.maxTotal); 	// 可分配实例数
		config.setMaxIdle(this.maxIdle);
		config.setMinIdle(this.minIdle);
		config.setMaxWaitMillis(this.maxWaitMillis);		// 当分配实例时，最大的等待时间
		
		nodes = new HashSet<String>();
		
		String[] hostAndPorts = servers.split(",");
		String[] server; 
		for(String hostAndPort : hostAndPorts) {
			server = hostAndPort.split(":");
			if(server.length < 2) {
				nodes.add(new HostAndPort(server[0], Protocol.DEFAULT_PORT).toString());
				continue;
			}
			
			nodes.add(new HostAndPort(server[0], Integer.valueOf(server[1])).toString());
		}
		
		connect();
	}
	

	public void connect()
	{
		jedisPool = new JedisSentinelPool(masterName, nodes, config);
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
			jedisPool.close();
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
