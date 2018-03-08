package com.hisunpay.redis.cluster;

import com.hisunpay.redis.RedisException;

/**
 * Redis 集群访问异常
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月24日 下午3:36:27
 *
 */
class RedisClusterException extends RedisException {

	/**  **/
	private static final long serialVersionUID = 1L;

	public RedisClusterException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public RedisClusterException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public RedisClusterException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}
}
