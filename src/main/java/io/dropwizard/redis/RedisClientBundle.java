package io.dropwizard.redis;

import brave.Tracing;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.lettuce.core.api.StatefulRedisConnection;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

public abstract class RedisClientBundle <K, V, T extends Configuration> implements ConfiguredBundle<T> {
    @Nullable
    private List<StatefulRedisConnection<K, V>> connections = new ArrayList<>();
    
    private int clientCount = 1;

    @Override
    public void initialize(final Bootstrap<?> bootstrap) {
        // do nothing
    }

    @Override
    public void run(final T configuration, final Environment environment) throws Exception {
        final RedisClientFactory<K, V> redisClientFactory = requireNonNull(getRedisClientFactory(configuration));

        final Tracing tracing = Tracing.current();

        for (int i = 0; i < clientCount; i++)
        	this.connections.add(redisClientFactory.build(
        			environment.healthChecks(), 
        			environment.lifecycle(), 
        			environment.metrics(), 
        			tracing, 
        			i));
    }

    public abstract RedisClientFactory<K, V> getRedisClientFactory(T configuration);
    
    public int getClientCount() {
		return clientCount;
	}
    
    public void setClientCount(int count) {
    	clientCount = count;
    }

    public List<StatefulRedisConnection<K, V>> getConnections() {
   		return requireNonNull(connections);
    }
    
    public StatefulRedisConnection<K, V> getConnection() {
    	return requireNonNull(connections.get(0));
    }
}
