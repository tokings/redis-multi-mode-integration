package com.hisunpay.redis;

import com.hisunpay.redis.cluster.RedisCluster;

import junit.framework.TestCase;

public class RedisClusterTester extends TestCase {
	
	private static RedisCluster cluster;

	@Override
	protected void setUp() throws Exception {
		cluster = new RedisCluster(
				"10.9.10.117:7001"
				+ ",10.9.10.117:7002"
				+ ",10.9.10.117:7003"
				+ ",10.9.10.117:7004"
				+ ",10.9.10.117:7005"
				+ ",10.9.10.117:7006"
				+ ",10.9.10.117:7007"
				+ ",10.9.10.117:7008"
			);
	}
	
	public void test() {
		String key = "tst_";
//		System.out.println(cluster.get(key));
//		Assert.assertTrue(cluster.incr(key) > 0);
//		System.out.println(cluster.get(key));
		
		// fill all slots in cluster
		long start = System.currentTimeMillis();
		int i = 0, max = 16383;
		System.out.println("start to set data...");
		while(i < max) {
			cluster.set(key + i, key + i);
			i ++;
		}
		
		System.out.println("data init finished. used " + (System.currentTimeMillis() - start) + "ms.");
	}

	@Override
	protected void tearDown() throws Exception {
		cluster.close();
	}
}
