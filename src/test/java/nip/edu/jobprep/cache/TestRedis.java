package nip.edu.jobprep.cache;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;


final class TestRedis {

    static final DockerImageName REDIS_IMAGE = DockerImageName.parse("redis:7-alpine");
    static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");

    private static final Network NETWORK = Network.newNetwork();

    static final GenericContainer<?> REDIS = new GenericContainer<>(REDIS_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("redis")
            .withExposedPorts(6379);

    static final ToxiproxyContainer TOXIPROXY = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(NETWORK);

    static final ToxiproxyContainer.ContainerProxy REDIS_PROXY;

    static {
        REDIS.start();
        TOXIPROXY.start();
        REDIS_PROXY = TOXIPROXY.getProxy("redis", 6379);
    }

    static String proxyAddress() {
        return REDIS_PROXY.getContainerIpAddress() + ":" + REDIS_PROXY.getProxyPort();
    }

    static String directAddress() {
        return REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    private TestRedis() {
    }
}