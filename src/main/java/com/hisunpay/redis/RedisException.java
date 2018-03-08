package com.hisunpay.redis;

/**
 * Redis 访问异常
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月24日 下午3:34:08
 *
 */
public class RedisException extends Exception {

	/**  **/
	private static final long serialVersionUID = 1L;

	public RedisException(String message, Throwable cause) {
		super(message, cause);
	}

	public RedisException(String message) {
		super(message);
	}

	public RedisException(Throwable cause) {
		super(cause);
	}
}
