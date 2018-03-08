package com.hisunpay.redis;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import junit.framework.TestCase;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

public class RedisSentinalTester extends TestCase {

	public void test() {
		Set sentinels = new HashSet();
		sentinels.add(new HostAndPort("10.9.10.117", 16379).toString());
//		sentinels.add(new HostAndPort("192.168.77.135", 26380).toString());
//		sentinels.add(new HostAndPort("192.168.77.135", 26381).toString());
		
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setBlockWhenExhausted(true);
		config.setMaxIdle(20);
		config.setMaxTotal(50);
		config.setMaxWaitMillis(10000);

		JedisSentinelPool sentinelPool = new JedisSentinelPool("AM117", sentinels, config);
		System.out.println("Current master: " + sentinelPool.getCurrentHostMaster().toString());
		
		Jedis master = sentinelPool.getResource();
		master.set("username", "tangdingzhi");
		master.close();
		
		Jedis master2 = sentinelPool.getResource();
		String value = master2.get("username");
		System.out.println("username: " + value);
		master2.close();
		sentinelPool.destroy();
	}
}
