package io.dropwizard.redis;

import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.Map;

import brave.Tracing;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.lettuce.core.api.StatefulRedisConnection;

public abstract class RedisClientBundle <K, V, T extends Configuration> implements ConfiguredBundle<T> {
    
	private RedisClientFactory<K, V> redisClientFactory;
	private Environment environment;
	
	private Map<Integer, StatefulRedisConnection<K, V>> connectionMap = new HashMap<>();

    @Override
    public void initialize(final Bootstrap<?> bootstrap) {
        // do nothing
    }

    @Override
    public void run(final T configuration, final Environment environment) throws Exception {
       redisClientFactory = requireNonNull(getRedisClientFactory(configuration));
       this.environment = environment;
    }

    public abstract RedisClientFactory<K, V> getRedisClientFactory(T configuration);

    public StatefulRedisConnection<K, V> getConnection() {
       return getConnection(0);
    }
    
    public StatefulRedisConnection<K, V> getConnection(int database) {
    	return connectionMap.computeIfAbsent(database, (i)->redisClientFactory.build(environment.healthChecks(), environment.lifecycle(), environment.metrics(), Tracing.current(), i));
    }
    
    
}
