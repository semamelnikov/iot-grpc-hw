package ru.kpfu.itis.grpc.server;

import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kpfu.itis.grpc.sqrt.Number;
import ru.kpfu.itis.grpc.sqrt.SqrtCalculationRequest;
import ru.kpfu.itis.grpc.sqrt.SqrtCalculationResponse;
import ru.kpfu.itis.grpc.sqrt.SqrtCalculationServiceGrpc;

public class SqrtCalculationService extends SqrtCalculationServiceGrpc.SqrtCalculationServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqrtCalculationService.class);

    @Override
    public void getSqrtOfNumber(SqrtCalculationRequest request, StreamObserver<SqrtCalculationResponse> responseObserver) {
        LOGGER.debug("*** Unary implementation on server side ***");
        final Number number = request.getNumber();
        final int value = number.getValue();
        LOGGER.debug("Request has been received on server side: number's value - {}", value);

        final double result = Math.sqrt(value);

        final SqrtCalculationResponse response = SqrtCalculationResponse.newBuilder()
                .setResult(result)
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
