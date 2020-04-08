package ru.kpfu.itis.grpc.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kpfu.itis.grpc.factorization.FactorizationRequest;
import ru.kpfu.itis.grpc.factorization.FactorizationServiceGrpc;
import ru.kpfu.itis.grpc.max.MaxCalculationRequest;
import ru.kpfu.itis.grpc.max.MaxCalculationResponse;
import ru.kpfu.itis.grpc.max.MaxCalculationServiceGrpc;
import ru.kpfu.itis.grpc.sqrt.Number;
import ru.kpfu.itis.grpc.sqrt.SqrtCalculationRequest;
import ru.kpfu.itis.grpc.sqrt.SqrtCalculationResponse;
import ru.kpfu.itis.grpc.sqrt.SqrtCalculationServiceGrpc;
import ru.kpfu.itis.grpc.std.StdCalculationRequest;
import ru.kpfu.itis.grpc.std.StdCalculationResponse;
import ru.kpfu.itis.grpc.std.StdCalculationServiceGrpc;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class GrpcClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcClient.class);

    private final String host = "localhost";
    private final int port = 5051;

    public static void main(String[] args) {
        LOGGER.debug("gRPC Client is started");
        final GrpcClient main = new GrpcClient();
        main.run();
    }

    private void run() {
        LOGGER.debug("Channel is created on {} with port: {}", host, port);
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        doUnaryCall(channel);

        doServerStreamingCall(channel);

        doClientStreamingCall(channel);

        doBiDiStreamingCall(channel);

        LOGGER.debug("Shutting down channel");
        channel.shutdown();
    }

    private void doServerStreamingCall(ManagedChannel channel) {
        LOGGER.debug("*** Server streaming implementation ***");
        final FactorizationServiceGrpc.FactorizationServiceBlockingStub factorizationClient =
                FactorizationServiceGrpc.newBlockingStub(channel);
        final int value = 132;
        final FactorizationRequest request = FactorizationRequest.newBuilder()
                .setNumber(
                        ru.kpfu.itis.grpc.factorization.Number.newBuilder()
                                .setValue(value)
                                .build()
                )
                .build();
        LOGGER.debug("Send number with value {}", value);
        LOGGER.debug("Factorization:");
        factorizationClient.getFactorizationOfNumber(request).forEachRemaining(factorizationResponse ->
                LOGGER.debug("{}", factorizationResponse.getResult().getValue())
        );
    }

    private void doBiDiStreamingCall(ManagedChannel channel) {
        LOGGER.debug("*** Bi directional streaming implementation ***");

        final int[] numbers = new int[]{3, 12, 4};

        final MaxCalculationServiceGrpc.MaxCalculationServiceStub asyncClient = MaxCalculationServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        final StreamObserver<MaxCalculationRequest> requestObserver = asyncClient.getMax(new StreamObserver<MaxCalculationResponse>() {
            @Override
            public void onNext(MaxCalculationResponse maxCalculationResponse) {
                LOGGER.debug("Response from the server: Current maximum is {}", maxCalculationResponse.getResult());
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOGGER.debug("Server is done sending data");
                latch.countDown();
            }
        });

        for (int i = 0; i < numbers.length; i++) {
            LOGGER.debug("Sending message #{}", i);
            requestObserver.onNext(
                    MaxCalculationRequest.newBuilder()
                            .setNumber(
                                    ru.kpfu.itis.grpc.max.Number.newBuilder()
                                            .setValue(numbers[i])
                                            .build()
                            )
                            .build()
            );
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        requestObserver.onCompleted();

        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doClientStreamingCall(ManagedChannel channel) {
        LOGGER.debug("*** Client streaming implementation ***");

        final int[] numbers = new int[]{3, 4, 12};

        final StdCalculationServiceGrpc.StdCalculationServiceStub asyncClient = StdCalculationServiceGrpc.newStub(channel);

        CountDownLatch latch = new CountDownLatch(1);

        final StreamObserver<StdCalculationRequest> requestObserver = asyncClient.getStandardDeviation(new StreamObserver<StdCalculationResponse>() {
            @Override
            public void onNext(StdCalculationResponse stdCalculationResponse) {
                LOGGER.debug(
                        "Received a response from the server. Standard deviation for {} is {}",
                        Arrays.toString(numbers),
                        stdCalculationResponse.getResult()
                );
            }

            @Override
            public void onError(Throwable throwable) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                LOGGER.debug("Server has completed sending us something");
                latch.countDown();
            }
        });

        for (int i = 0; i < numbers.length; i++) {
            LOGGER.debug("Sending message #{}", i);
            requestObserver.onNext(
                    StdCalculationRequest.newBuilder()
                            .setNumber(
                                    ru.kpfu.itis.grpc.std.Number.newBuilder()
                                            .setValue(numbers[i])
                                            .build()
                            )
                            .build()
            );
        }
        requestObserver.onCompleted();

        try {
            latch.await(3L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void doUnaryCall(final ManagedChannel channel) {
        LOGGER.debug("*** Unary implementation ***");
        SqrtCalculationServiceGrpc.SqrtCalculationServiceBlockingStub sqrtCalculationClient =
                SqrtCalculationServiceGrpc.newBlockingStub(channel);

        final int value = 36;

        final Number number = Number.newBuilder()
                .setValue(value)
                .build();

        final SqrtCalculationRequest request = SqrtCalculationRequest.newBuilder()
                .setNumber(number)
                .build();


        final SqrtCalculationResponse sqrtCalculationResponse = sqrtCalculationClient.getSqrtOfNumber(request);

        LOGGER.info("Response has been received. Sqrt of value {} is {}", value, sqrtCalculationResponse.getResult());
    }
}
