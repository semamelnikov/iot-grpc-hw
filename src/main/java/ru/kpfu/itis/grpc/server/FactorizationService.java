package ru.kpfu.itis.grpc.server;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kpfu.itis.grpc.factorization.FactorizationRequest;
import ru.kpfu.itis.grpc.factorization.FactorizationResponse;
import ru.kpfu.itis.grpc.factorization.FactorizationServiceGrpc;
import ru.kpfu.itis.grpc.factorization.Number;

import java.util.ArrayList;
import java.util.List;

public class FactorizationService extends FactorizationServiceGrpc.FactorizationServiceImplBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(FactorizationService.class);

    @Override
    public void getFactorizationOfNumber(FactorizationRequest request, StreamObserver<FactorizationResponse> responseObserver) {
        LOGGER.debug("*** Server streaming implementation on server side ***");
        final Number number = request.getNumber();
        final int value = number.getValue();

        final List<Integer> result = getFactorization(value);
        try {
            for (final Integer prime : result) {
                final FactorizationResponse response = FactorizationResponse.newBuilder()
                        .setResult(
                                Number.newBuilder()
                                        .setValue(prime)
                                        .build()
                        )
                        .build();
                LOGGER.debug("Send current prime number {}", prime);
                responseObserver.onNext(response);
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            LOGGER.debug("all messages have been sent");
            responseObserver.onCompleted();

        }
    }

    private List<Integer> getFactorization(final int value) {
        int n = value;
        final List<Integer> factors = new ArrayList<>();
        for (int i = 2; i <= n / i; i++) {
            while (n % i == 0) {
                factors.add(i);
                n /= i;
            }
        }
        if (n > 1) {
            factors.add(n);
        }
        return factors;
    }
}
