package nip.edu.jobprep.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Infrastructure smoke test: Redis reachable through Toxiproxy, basic commands
 * and EVAL with TIME work, and a proxy cutoff surfaces as a connection failure.
 * Uses raw RESP over a socket: no Redis client dependency is added in this
 * subtask (the client arrives with the Redis integration subtask).
 */
class RedisInfraSmokeIT {

    @Test
    void redisReachableThroughProxyAndEvalTimeWorks() throws Exception {
        String[] hp = TestRedis.proxyAddress().split(":");
        try (var socket = new java.net.Socket(hp[0], Integer.parseInt(hp[1]))) {
            socket.setSoTimeout(5000);
            var out = socket.getOutputStream();
            var in = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream()));

            // PING
            out.write("*1\r\n$4\r\nPING\r\n".getBytes());
            assertThat(in.readLine()).isEqualTo("+PONG");

            // HSET / HGET roundtrip
            out.write("*4\r\n$4\r\nHSET\r\n$4\r\nacct\r\n$3\r\nbal\r\n$3\r\n100\r\n".getBytes());
            assertThat(in.readLine()).isEqualTo(":1");
            out.write("*3\r\n$4\r\nHGET\r\n$4\r\nacct\r\n$3\r\nbal\r\n".getBytes());
            assertThat(in.readLine()).isEqualTo("$3");
            assertThat(in.readLine()).isEqualTo("100");

            // EVAL with TIME inside the script (invariant I-6 of the cache design).
            // RESP2: a Lua number returned from a script comes back as a bulk string.
            String script = "return redis.call('TIME')[1]";
            out.write(("*3\r\n$4\r\nEVAL\r\n$" + script.length() + "\r\n" + script + "\r\n$1\r\n0\r\n").getBytes());
            assertThat(in.readLine()).startsWith("$");     // bulk string header
            String seconds = in.readLine();                 // unix epoch seconds
            assertThat(Long.parseLong(seconds)).isPositive();
        }
    }

    @Test
    void proxyCutoffSurfacesAsConnectionFailure() throws Exception {
        String[] hp = TestRedis.proxyAddress().split(":");
        var proxy = TestRedis.REDIS_PROXY;

        try (var socket = new java.net.Socket(hp[0], Integer.parseInt(hp[1]))) {
            socket.setSoTimeout(5000);
            socket.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes());
            assertThat(new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream())).readLine())
                    .isEqualTo("+PONG");
        }

        // Cut all existing and new connections to Redis
        proxy.setConnectionCut(true);

        await("connection refused while proxy cut")
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThatThrownBy(() -> {
                    try (var s = new java.net.Socket(hp[0], Integer.parseInt(hp[1]))) {
                        s.setSoTimeout(2000);
                        s.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes());
                        s.getInputStream().read();
                    }
                }).isInstanceOf(Exception.class));

        proxy.setConnectionCut(false);

        await("connection restored after proxy uncut")
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    try (var s = new java.net.Socket(hp[0], Integer.parseInt(hp[1]))) {
                        s.setSoTimeout(5000);
                        s.getOutputStream().write("*1\r\n$4\r\nPING\r\n".getBytes());
                        assertThat(new java.io.BufferedReader(new java.io.InputStreamReader(s.getInputStream())).readLine())
                                .isEqualTo("+PONG");
                    }
                });
    }
}