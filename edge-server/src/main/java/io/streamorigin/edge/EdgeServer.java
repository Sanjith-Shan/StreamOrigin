package io.streamorigin.edge;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The serve path: edge caches GET segments here. Runs as {@code role=edge} next to a separate
 * publish-server, or as {@code role=combined} with both paths in one process.
 */
@SpringBootApplication(scanBasePackages = "io.streamorigin")
public class EdgeServer {

    public static void main(String[] args) {
        SpringApplication.run(EdgeServer.class, args);
    }
}
