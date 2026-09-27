package io.streamorigin.publish;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** The publish path: packagers PUT segments here. Its own process, its own store connections. */
@SpringBootApplication(scanBasePackages = "io.streamorigin")
public class PublishServer {

    public static void main(String[] args) {
        SpringApplication.run(PublishServer.class, args);
    }
}
