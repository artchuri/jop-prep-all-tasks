package nip.edu.jobprep.cache;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared Redis + Toxiproxy containers for cache integration tests.
 * All traffic to Redis goes through the Toxiproxy proxy so tests can inject
 * network failures (latency, cutoffs) without restarting containers.
 * Both containers share one Docker network: toxiproxy reaches Redis by
 * network alias, tests reach the proxy from the host.
 */
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

    /** Host:port of Redis as seen from the host through the proxy. */
    static String proxyAddress() {
        return REDIS_PROXY.getContainerIpAddress() + ":" + REDIS_PROXY.getProxyPort();
    }

    /** Host:port of Redis bypassing the proxy (host-mapped direct port). */
    static String directAddress() {
        return REDIS.getHost() + ":" + REDIS.getMappedPort(6379);
    }

    private TestRedis() {
    }
}