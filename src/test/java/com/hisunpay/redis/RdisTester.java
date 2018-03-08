package com.hisunpay.redis;

import com.hisunpay.redis.single.RedisClient;

import junit.framework.Assert;
import junit.framework.TestCase;
import redis.clients.jedis.Protocol;

public class RdisTester extends TestCase {

	RedisClient client = new RedisClient("10.9.10.117", Protocol.DEFAULT_PORT);

	@Override
	protected void setUp() throws Exception {
		client.init();
//		RedisServer.getInstance().start("10.9.10.117", 6371);
	}

	@Override
	protected void tearDown() throws Exception {
		client.close();
	}
	
	public void test() {
//		client.incr("test");
		Assert.assertEquals(client.get("test"), "3");
	}
}
