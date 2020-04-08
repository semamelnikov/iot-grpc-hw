package ru.kpfu.itis.grpc.server;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kpfu.itis.grpc.max.MaxCalculationRequest;
import ru.kpfu.itis.grpc.max.MaxCalculationResponse;
import ru.kpfu.itis.grpc.max.MaxCalculationServiceGrpc;
import ru.kpfu.itis.grpc.max.Number;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MaximumService extends MaxCalculationServiceGrpc.MaxCalculationServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaximumService.class);

    @Override
    public StreamObserver<MaxCalculationRequest> getMax(StreamObserver<MaxCalculationResponse> responseObserver) {
        LOGGER.debug("*** Bi directional streaming implementation on server side ***");

        final StreamObserver<MaxCalculationRequest> requestObserver = new StreamObserver<MaxCalculationRequest>() {

            final List<Integer> numbers = new ArrayList<>();

            @Override
            public void onNext(MaxCalculationRequest maxCalculationRequest) {

                numbers.add(
                        maxCalculationRequest.getNumber().getValue()
                );

                final int max = Collections.max(numbers);

                final MaxCalculationResponse response = MaxCalculationResponse.newBuilder()
                        .setResult(
                                Number.newBuilder()
                                        .setValue(max)
                                        .build()
                        )
                        .build();

                LOGGER.debug("Current max for {} is {}", numbers.toString(), max);
                responseObserver.onNext(response);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                numbers.clear();
                LOGGER.debug("close bi directional streaming");
                responseObserver.onCompleted();
            }
        };
        return requestObserver;
    }
}
