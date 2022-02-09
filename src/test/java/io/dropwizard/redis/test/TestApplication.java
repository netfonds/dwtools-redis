package io.dropwizard.redis.test;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.redis.RedisClientBundle;
import io.dropwizard.redis.RedisClientFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.lettuce.core.api.StatefulRedisConnection;

public class TestApplication extends Application<TestConfiguration> {

    private final RedisClientBundle<String, String, TestConfiguration> redisCluster =
            new RedisClientBundle<String, String, TestConfiguration>() {
        @Override
        public RedisClientFactory<String, String> getRedisClientFactory(final TestConfiguration configuration) {
            return configuration.getRedisClientFactory();
        }
    };

    @Override
    public void initialize(Bootstrap<TestConfiguration> bootstrap) {
        bootstrap.addBundle(redisCluster);
    }

    @Override
    public void run(final TestConfiguration testConfiguration, final Environment environment) throws Exception {
        final StatefulRedisConnection<String, String> clusterConnection = redisCluster.getConnection();

        clusterConnection.sync().set("foo", "bar");
    }

    public RedisClientBundle<String, String, TestConfiguration> getRedisCluster() {
        return redisCluster;
    }
}
