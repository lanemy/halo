/*
 * Project: halo
 *
 * File Created at 2020年12月04日
 *
 */
package run.halo.app.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import run.halo.app.config.properties.HaloProperties;
import run.halo.app.utils.JsonUtils;

import javax.annotation.PreDestroy;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Type StandaloneRedisCacheStore
 * @Desc StandaloneRedisCacheStore
 * @author lanemy
 * @date 2020年12月04日 17:44:11
 * @version
 */
@Slf4j
public class StandaloneRedisCacheStore extends AbstractStringCacheStore{

	private volatile static JedisPool JEDISPOOL;

	/**
	 * Lock.
	 */
	private final Lock lock = new ReentrantLock();

	public StandaloneRedisCacheStore(HaloProperties haloProperties) {
		this.haloProperties = haloProperties;
		initRedis();
	}

	private void initRedis() {
		JedisPoolConfig cfg = new JedisPoolConfig();
		cfg.setMaxIdle(10);
		cfg.setMaxTotal(50);
		cfg.setMaxWaitMillis(1000);
		cfg.setTestOnBorrow(true);

		JEDISPOOL = new JedisPool(cfg, this.haloProperties.getCacheRedisHost(), this.haloProperties.getCacheRedisPort(), 5, this.haloProperties.getCacheRedisPassword());

		log.info("Initialized cache redis standalone mode, host:{}, port:{}", this.haloProperties.getCacheRedisHost(), this.haloProperties.getCacheRedisPort());

	}

	@NotNull
	@Override
	Optional<CacheWrapper<String>> getInternal(@NotNull String key) {
		Assert.hasText(key, "Cache key must not be blank");
		Jedis jedis = JEDISPOOL.getResource();
		String v = jedis.get(key);
		return StringUtils.isEmpty(v) ? Optional.empty() : jsonToCacheWrapper(v);
	}

	@Override
	void putInternal(@NotNull String key, @NotNull CacheWrapper<String> cacheWrapper) {
		putInternalIfAbsent(key, cacheWrapper);
		try {
			Jedis jedis = JEDISPOOL.getResource();
			jedis.set(key, JsonUtils.objectToJson(cacheWrapper));
			Date ttl = cacheWrapper.getExpireAt();
			if (ttl != null) {
				jedis.pexpireAt(key, ttl.getTime());
			}
		} catch (Exception e) {
			log.warn("Put cache fail json2object key: [{}] value:[{}]", key, cacheWrapper);
		}
	}

	@Override
	Boolean putInternalIfAbsent(@NotNull String key, @NotNull CacheWrapper<String> cacheWrapper) {
		Assert.hasText(key, "Cache key must not be blank");
		Assert.notNull(cacheWrapper, "Cache wrapper must not be null");
		try {
			Jedis jedis = JEDISPOOL.getResource();
			if (jedis.setnx(key, JsonUtils.objectToJson(cacheWrapper)) <= 0) {
				log.warn("Failed to put the cache, because the key: [{}] has been present already", key);
				return false;
			}
			Date ttl = cacheWrapper.getExpireAt();
			if (ttl != null) {
				jedis.pexpireAt(key, ttl.getTime());
			}
			return true;
		} catch (JsonProcessingException e) {
			log.warn("Put cache fail json2object key: [{}] value:[{}]", key, cacheWrapper);
		}
		log.debug("Cache key: [{}], original cache wrapper: [{}]", key, cacheWrapper);
		return false;
	}

	@Override
	public void delete(@NotNull String key) {
		Assert.hasText(key, "Cache key must not be blank");
		Jedis jedis = JEDISPOOL.getResource();
		jedis.del(key);
		log.debug("Removed key: [{}]", key);
	}

	@PreDestroy
	public void preDestroy() {
	}
}
/*
 * Revision history
 * -------------------------------------------------------------------------
 *
 * Date Author Note
 * -------------------------------------------------------------------------
 * 2020年12月04日 17:44:11 lane create
 */