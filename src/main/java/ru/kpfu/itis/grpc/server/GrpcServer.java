package ru.kpfu.itis.grpc.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class GrpcServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcServer.class);

    public static void main(String[] args) throws InterruptedException, IOException {
        LOGGER.info("gRPC Server is started");

        final Server server = ServerBuilder.forPort(5051)
                .addService(new SqrtCalculationService())
                .addService(new StandardDeviationService())
                .addService(new MaximumService())
                .addService(new FactorizationService())
                .build();

        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Received Shutdown Request");
            server.shutdown();
            System.out.println("Successfully stopped the server");
        }));

        server.awaitTermination();
    }
}
