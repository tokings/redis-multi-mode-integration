package com.hisunpay.redis;

import com.hisunpay.redis.sentinal.RedisRentinalClient;

import junit.framework.TestCase;
import redis.clients.jedis.Jedis;

public class RedisSentinalTester1 extends TestCase {

	public void test() {
		RedisRentinalClient client = new RedisRentinalClient("AM117", "10.9.10.117:16379", 50, 20, 0, 10000);
		
		Jedis jedis = client.getResurce();
		
		System.out.println(jedis.incr("test"));
		System.out.println(jedis.incr("test"));
		
		client.returnResource(jedis);
		
		client.close();
	}
}
