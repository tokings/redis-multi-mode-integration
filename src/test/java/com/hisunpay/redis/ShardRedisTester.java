package com.hisunpay.redis;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import junit.framework.TestCase;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class ShardRedisTester extends TestCase {

	public void test() {
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setBlockWhenExhausted(true);
		config.setMaxIdle(20);
		config.setMaxTotal(50);
		config.setMaxWaitMillis(10000);
		
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		shards.add(new JedisShardInfo("10.9.10.117", 6371));
		shards.add(new JedisShardInfo("10.9.10.117", 6379));
		shards.add(new JedisShardInfo("10.9.10.117", 6372));
		
		ShardedJedisPool pool = new ShardedJedisPool(config, shards );
		
		ShardedJedis jedis = pool.getResource();
		jedis.set("username", "tdz");
		System.out.println(jedis.get("username"));
		jedis.close();
		pool.close();
		pool.destroy();
	}
}
