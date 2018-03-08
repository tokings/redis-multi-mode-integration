package com.hisunpay.redis.cluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.hisunpay.redis.RedisException;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitPosParams;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;

/**
 * Redis 集群操作
 * <p>实现了所有Jedis接口</p>
 * @author tdz
 * @Copyright (C)2016 , hisunpay 高阳通联
 * @website http://www.hisunpay.com
 * @date 2016年11月24日 下午3:18:06
 *
 */
public class RedisCluster implements JedisCommands {

	/**
	 * 集群节点
	 */
	private Set<HostAndPort> nodes;
	/**
	 * 集群连接
	 */
	private JedisCluster cluster;
	
	private int maxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;
	private int maxIdle = GenericObjectPoolConfig.DEFAULT_MAX_IDLE;
	private int minIdle = GenericObjectPoolConfig.DEFAULT_MIN_IDLE;
	private long maxWaitMillis = GenericObjectPoolConfig.DEFAULT_MAX_WAIT_MILLIS;
	
	public RedisCluster() {
		
		GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
		conf.setMaxIdle(maxIdle);
		conf.setMinIdle(minIdle);
		conf.setMaxTotal(maxTotal);
		conf.setMaxWaitMillis(maxWaitMillis);
		
		nodes = new HashSet<HostAndPort>();
		nodes.add(new HostAndPort(Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT));
		
		cluster = new JedisCluster(nodes, conf);
	}
	
	/**
	 * Redis 集群操作
	 * @param servers 集群节点列表	{eg:127.0.0.1:6379,127.0.0.1:6380}
	 * <br>
	 * 
	 */
	public RedisCluster(String servers) {
		
		GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
		conf.setMaxIdle(maxIdle);
		conf.setMinIdle(minIdle);
		conf.setMaxTotal(maxTotal);
		conf.setMaxWaitMillis(maxWaitMillis);
		
		nodes = new HashSet<HostAndPort>();
		
		String[] hostAndPorts = servers.split(",");
		String[] server; 
		for(String hostAndPort : hostAndPorts) {
			server = hostAndPort.split(":");
			if(server.length < 2) {
				nodes.add(new HostAndPort(server[0], Protocol.DEFAULT_PORT));
				continue;
			}
			
			nodes.add(new HostAndPort(server[0], Integer.valueOf(server[1])));
		}
		
		cluster = new JedisCluster(nodes, conf);
	}

	/**
	 * Redis 集群操作
	 * @param servers 集群节点列表	{eg:127.0.0.1:6379,127.0.0.1:6380}
	 * @param maxTotal	最大连接数
	 * @param maxIdle 最大空闲数
	 * @param minIdle 最小空闲数
	 * @param maxWaitMillis 超时时间长度
	 */
	public RedisCluster(String servers, int maxTotal, int maxIdle, int minIdle, long maxWaitMillis) {
		
		this.maxTotal = maxTotal;
		this.maxIdle = maxIdle;
		this.minIdle = minIdle;
		this.maxWaitMillis = maxWaitMillis;
		
		GenericObjectPoolConfig conf = new GenericObjectPoolConfig();
		conf.setMaxIdle(maxIdle);
		conf.setMinIdle(minIdle);
		conf.setMaxTotal(maxTotal);
		conf.setMaxWaitMillis(maxWaitMillis);
		
		nodes = new HashSet<HostAndPort>();
		
		String[] hostAndPorts = servers.split(",");
		String[] server; 
		for(String hostAndPort : hostAndPorts) {
			server = hostAndPort.split(":");
			if(server.length < 2) {
				nodes.add(new HostAndPort(server[0], Protocol.DEFAULT_PORT));
				continue;
			}
			
			nodes.add(new HostAndPort(server[0], Integer.valueOf(server[1])));
		}
		
		cluster = new JedisCluster(nodes, conf);
	}
	
	/**
	 * 关闭集群
	 * @throws RedisException
	 */
	public void close() throws RedisException {
		try {
			this.cluster.close();
		} catch (IOException e) {
			throw new RedisClusterException(e);
		}
	}
	
	public JedisCluster getJedisCluster() {
		return cluster;
	}

	public String set(String key, String value) {
		return this.cluster.set(key, value);
	}

	public String set(String key, String value, String nxxx, String expx, long time) {
		return this.cluster.set(key, value, nxxx, expx, time);
	}

	@Deprecated
	public String set(String key, String value, String nxxx) {
		return this.cluster.set(key, value, nxxx);
	}

	public String get(String key) {
		return this.cluster.get(key);
	}

	public Boolean exists(String key) {
		return this.cluster.exists(key);
	}

	public Long persist(String key) {
		return this.cluster.persist(key);
	}

	public String type(String key) {
		return this.cluster.type(key);
	}

	public Long expire(String key, int seconds) {
		return this.cluster.expire(key, seconds);
	}

	public Long pexpire(String key, long milliseconds) {
		return this.cluster.expireAt(key, milliseconds);
	}

	public Long expireAt(String key, long unixTime) {
		return this.cluster.expireAt(key, unixTime);
	}

	public Long pexpireAt(String key, long millisecondsTimestamp) {
		return this.cluster.pexpireAt(key, millisecondsTimestamp);
	}

	public Long ttl(String key) {
		return this.cluster.ttl(key);
	}

	public Long pttl(String key) {
		return this.cluster.pttl(key);
	}

	public Boolean setbit(String key, long offset, boolean value) {
		return this.cluster.setbit(key, offset, value);
	}

	public Boolean setbit(String key, long offset, String value) {
		return this.cluster.setbit(key, offset, value);
	}

	public Boolean getbit(String key, long offset) {
		return this.cluster.getbit(key, offset);
	}

	public Long setrange(String key, long offset, String value) {
		return this.cluster.setrange(key, offset, value);
	}

	public String getrange(String key, long startOffset, long endOffset) {
		return this.cluster.getrange(key, startOffset, endOffset);
	}

	public String getSet(String key, String value) {
		return this.cluster.getSet(key, value);
	}

	public Long setnx(String key, String value) {
		return this.cluster.setnx(key, value);
	}

	public String setex(String key, int seconds, String value) {
		return this.cluster.setex(key, seconds, value);
	}

	public String psetex(String key, long milliseconds, String value) {
		return this.cluster.psetex(key, milliseconds, value);
	}

	public Long decrBy(String key, long integer) {
		return this.cluster.decrBy(key, integer);
	}

	public Long decr(String key) {
		return this.cluster.decr(key);
	}

	public Long incrBy(String key, long integer) {
		return this.cluster.incrBy(key, integer);
	}

	public Double incrByFloat(String key, double value) {
		return this.cluster.incrByFloat(key, value);
	}

	public Long incr(String key) {
		return this.cluster.incr(key);
	}

	public Long append(String key, String value) {
		return this.cluster.append(key, value);
	}

	public String substr(String key, int start, int end) {
		return this.cluster.substr(key, start, end);
	}

	public Long hset(String key, String field, String value) {
		return this.cluster.hset(key, field, value);
	}

	public String hget(String key, String field) {
		return this.cluster.hget(key, field);
	}

	public Long hsetnx(String key, String field, String value) {
		return this.cluster.hsetnx(key, field, value);
	}

	public String hmset(String key, Map<String, String> hash) {
		return this.cluster.hmset(key, hash);
	}

	public List<String> hmget(String key, String... fields) {
		return this.cluster.hmget(key, fields);
	}

	public Long hincrBy(String key, String field, long value) {
		return this.cluster.hincrBy(key, field, value);
	}

	public Double hincrByFloat(String key, String field, double value) {
		return this.cluster.hincrByFloat(key, field, value);
	}

	public Boolean hexists(String key, String field) {
		return this.cluster.hexists(key, field);
	}

	public Long hdel(String key, String... field) {
		return this.cluster.hdel(key, field);
	}

	public Long hlen(String key) {
		return this.cluster.hlen(key);
	}

	public Set<String> hkeys(String key) {
		return this.cluster.hkeys(key);
	}

	public List<String> hvals(String key) {
		return this.cluster.hvals(key);
	}

	public Map<String, String> hgetAll(String key) {
		return this.cluster.hgetAll(key);
	}

	public Long rpush(String key, String... string) {
		return this.cluster.rpush(key, string);
	}

	public Long lpush(String key, String... string) {
		return this.cluster.lpush(key, string);
	}

	public Long llen(String key) {
		return this.cluster.llen(key);
	}

	public List<String> lrange(String key, long start, long end) {
		return this.cluster.lrange(key, start, end);
	}

	public String ltrim(String key, long start, long end) {
		return this.cluster.ltrim(key, start, end);
	}

	public String lindex(String key, long index) {
		return this.cluster.lindex(key, index);
	}

	public String lset(String key, long index, String value) {
		return this.cluster.lset(key, index, value);
	}

	public Long lrem(String key, long count, String value) {
		return this.cluster.lrem(key, count, value);
	}

	public String lpop(String key) {
		return this.cluster.lpop(key);
	}

	public String rpop(String key) {
		return this.cluster.rpop(key);
	}

	public Long sadd(String key, String... member) {
		return this.cluster.sadd(key, member);
	}

	public Set<String> smembers(String key) {
		return this.cluster.smembers(key);
	}

	public Long srem(String key, String... member) {
		return this.cluster.srem(key, member);
	}

	public String spop(String key) {
		return this.cluster.spop(key);
	}

	public Set<String> spop(String key, long count) {
		return this.cluster.spop(key, count);
	}

	public Long scard(String key) {
		return this.cluster.scard(key);
	}

	public Boolean sismember(String key, String member) {
		return this.cluster.sismember(key, member);
	}

	public String srandmember(String key) {
		return this.cluster.srandmember(key);
	}

	public List<String> srandmember(String key, int count) {
		return this.cluster.srandmember(key, count);
	}

	public Long strlen(String key) {
		return this.cluster.strlen(key);
	}

	public Long zadd(String key, double score, String member) {
		return this.cluster.zadd(key, score, member);
	}

	public Long zadd(String key, double score, String member, ZAddParams params) {
		return this.cluster.zadd(key, score, member, params);
	}

	public Long zadd(String key, Map<String, Double> scoreMembers) {
		return this.cluster.zadd(key, scoreMembers);
	}

	public Long zadd(String key, Map<String, Double> scoreMembers, ZAddParams params) {
		return this.cluster.zadd(key, scoreMembers, params);
	}

	public Set<String> zrange(String key, long start, long end) {
		return this.cluster.zrange(key, start, end);
	}

	public Long zrem(String key, String... member) {
		return this.cluster.zrem(key, member);
	}

	public Double zincrby(String key, double score, String member) {
		return this.cluster.zincrby(key, score, member);
	}

	public Double zincrby(String key, double score, String member, ZIncrByParams params) {
		return this.cluster.zincrby(key, score, member, params);
	}

	public Long zrank(String key, String member) {
		return this.cluster.zrank(key, member);
	}

	public Long zrevrank(String key, String member) {
		return this.cluster.zrevrank(key, member);
	}

	public Set<String> zrevrange(String key, long start, long end) {
		return this.cluster.zrevrange(key, start, end);
	}

	public Set<Tuple> zrangeWithScores(String key, long start, long end) {
		return this.cluster.zrangeWithScores(key, start, end);
	}

	public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
		return this.cluster.zrevrangeWithScores(key, start, end);
	}

	public Long zcard(String key) {
		return this.cluster.zcard(key);
	}

	public Double zscore(String key, String member) {
		return this.cluster.zscore(key, member);
	}

	public List<String> sort(String key) {
		return this.cluster.sort(key);
	}

	public List<String> sort(String key, SortingParams sortingParameters) {
		return this.cluster.sort(key, sortingParameters);
	}

	public Long zcount(String key, double min, double max) {
		return this.cluster.zcount(key, min, max);
	}

	public Long zcount(String key, String min, String max) {
		return this.cluster.zcount(key, min, max);
	}

	public Set<String> zrangeByScore(String key, double min, double max) {
		return this.cluster.zrangeByScore(key, min, max);
	}

	public Set<String> zrangeByScore(String key, String min, String max) {
		return this.cluster.zrangeByScore(key, min, max);
	}

	public Set<String> zrevrangeByScore(String key, double max, double min) {
		return this.cluster.zrevrangeByScore(key, max, min);
	}

	public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
		return this.cluster.zrangeByScore(key, min, max, offset,count);
	}

	public Set<String> zrevrangeByScore(String key, String max, String min) {
		return this.cluster.zrevrangeByScore(key, max, min);
	}

	public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
		return this.cluster.zrangeByScore(key, min, max, offset, count);
	}

	public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		return this.cluster.zrevrangeByScore(key, max, min, offset, count);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		return this.cluster.zrangeByScoreWithScores(key, min, max);
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		return this.cluster.zrevrangeByScoreWithScores(key, max, min);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		return this.cluster.zrangeByScoreWithScores(key, min, max, offset, count);
	}

	public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
		return this.cluster.zrevrangeByScore(key, max, min, offset, count);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
		return this.cluster.zrangeByScoreWithScores(key, min, max);
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
		return this.cluster.zrevrangeByScoreWithScores(key, max, min);
	}

	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
		return this.cluster.zrangeByScoreWithScores(key, max, min, offset, count);
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		return this.cluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
		return this.cluster.zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	public Long zremrangeByRank(String key, long start, long end) {
		return this.cluster.zremrangeByRank(key, start, end);
	}

	public Long zremrangeByScore(String key, double start, double end) {
		return this.cluster.zremrangeByScore(key, start, end);
	}

	public Long zremrangeByScore(String key, String start, String end) {
		return this.cluster.zremrangeByScore(key, start, end);
	}

	public Long zlexcount(String key, String min, String max) {
		return this.cluster.zlexcount(key, min, max);
	}

	public Set<String> zrangeByLex(String key, String min, String max) {
		return this.cluster.zrangeByLex(key, min, max);
	}

	public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
		return this.cluster.zrangeByLex(key, min, max, offset, count);
	}

	public Set<String> zrevrangeByLex(String key, String max, String min) {
		return this.cluster.zrevrangeByLex(key, max, min);
	}

	public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
		return this.cluster.zrevrangeByLex(key, max, min, offset, count);
	}

	public Long zremrangeByLex(String key, String min, String max) {
		return this.cluster.zremrangeByLex(key, min, max);
	}

	public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
		return this.cluster.linsert(key, where, pivot, value);
	}

	public Long lpushx(String key, String... string) {
		return this.cluster.lpushx(key, string);
	}

	public Long rpushx(String key, String... string) {
		return this.cluster.rpushx(key, string);
	}

	@Deprecated
	public List<String> blpop(String arg) {
		return this.cluster.blpop(arg);
	}

	public List<String> blpop(int timeout, String key) {
		return this.cluster.blpop(timeout, key);
	}

	@Deprecated
	public List<String> brpop(String arg) {
		return this.cluster.brpop(arg);
	}

	public List<String> brpop(int timeout, String key) {
		return this.cluster.brpop(timeout, key);
	}

	public Long del(String key) {
		return this.cluster.del(key);
	}

	public String echo(String string) {
		return this.cluster.echo(string);
	}

	@Deprecated
	public Long move(String key, int dbIndex) {
		return this.cluster.move(key, dbIndex);
	}

	public Long bitcount(String key) {
		return this.cluster.bitcount(key);
	}

	public Long bitcount(String key, long start, long end) {
		return this.cluster.bitcount(key, start, end);
	}

	public Long bitpos(String key, boolean value) {
		return this.cluster.bitpos(key, value);
	}

	public Long bitpos(String key, boolean value, BitPosParams params) {
		return this.cluster.bitpos(key, value, params);
	}

	@Deprecated
	public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
		return this.cluster.hscan(key, cursor);
	}

	@Deprecated
	public ScanResult<String> sscan(String key, int cursor) {
		return this.cluster.sscan(key, cursor);
	}

	@Deprecated
	public ScanResult<Tuple> zscan(String key, int cursor) {
		return this.cluster.zscan(key, cursor);
	}

	public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
		return this.cluster.hscan(key, cursor);
	}

	public ScanResult<Entry<String, String>> hscan(String key, String cursor, ScanParams params) {
		return this.cluster.hscan(key, cursor, params);
	}

	public ScanResult<String> sscan(String key, String cursor) {
		return this.cluster.sscan(key, cursor);
	}

	public ScanResult<String> sscan(String key, String cursor, ScanParams params) {
		return this.cluster.sscan(key, cursor, params);
	}

	public ScanResult<Tuple> zscan(String key, String cursor) {
		return this.cluster.zscan(key, cursor);
	}

	public ScanResult<Tuple> zscan(String key, String cursor, ScanParams params) {
		return this.cluster.zscan(key, cursor, params);
	}

	public Long pfadd(String key, String... elements) {
		return this.cluster.pfadd(key, elements);
	}

	public long pfcount(String key) {
		return this.cluster.pfcount(key);
	}

	public Long geoadd(String key, double longitude, double latitude, String member) {
		return this.cluster.geoadd(key, longitude, latitude, member);
	}

	public Long geoadd(String key, Map<String, GeoCoordinate> memberCoordinateMap) {
		return this.cluster.geoadd(key, memberCoordinateMap);
	}

	public Double geodist(String key, String member1, String member2) {
		return this.cluster.geodist(key, member1, member2);
	}

	public Double geodist(String key, String member1, String member2, GeoUnit unit) {
		return this.cluster.geodist(key, member1, member2, unit);
	}

	public List<String> geohash(String key, String... members) {
		return this.cluster.geohash(key, members);
	}

	public List<GeoCoordinate> geopos(String key, String... members) {
		return this.cluster.geopos(key, members);
	}

	public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius,
			GeoUnit unit) {
		return this.cluster.georadius(key, longitude, latitude, radius, unit);
	}

	public List<GeoRadiusResponse> georadius(String key, double longitude, double latitude, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		return this.cluster.georadius(key, longitude, latitude, radius, unit, param);
	}

	public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit) {
		return this.cluster.georadiusByMember(key, member, radius, unit);
	}

	public List<GeoRadiusResponse> georadiusByMember(String key, String member, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		return this.cluster.georadiusByMember(key, member, radius, unit, param);
	}

	public List<Long> bitfield(String key, String... arguments) {
		return this.cluster.bitfield(key, arguments);
	}
	
}
