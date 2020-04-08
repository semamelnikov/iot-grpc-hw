package ru.kpfu.itis.grpc.server;

import io.grpc.stub.StreamObserver;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.kpfu.itis.grpc.std.StdCalculationRequest;
import ru.kpfu.itis.grpc.std.StdCalculationResponse;
import ru.kpfu.itis.grpc.std.StdCalculationServiceGrpc;

import java.util.ArrayList;
import java.util.List;

public class StandardDeviationService extends StdCalculationServiceGrpc.StdCalculationServiceImplBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(SqrtCalculationService.class);

    private final StandardDeviation standardDeviation = new StandardDeviation();

    @Override
    public StreamObserver<StdCalculationRequest> getStandardDeviation(StreamObserver<StdCalculationResponse> responseObserver) {
        LOGGER.debug("*** Client streaming implementation on server side ***");

        final StreamObserver<StdCalculationRequest> requestStreamObserver = new StreamObserver<StdCalculationRequest>() {
            final List<Double> numbers = new ArrayList<>();

            @Override
            public void onNext(StdCalculationRequest stdCalculationRequest) {
                final double currentValue = stdCalculationRequest.getNumber().getValue();
                LOGGER.debug("Number with value {} is processed", currentValue);
                numbers.add(currentValue);
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {
                final double standardDeviationResult = standardDeviation.evaluate(getNumbersArray());

                final StdCalculationResponse response = StdCalculationResponse
                        .newBuilder()
                        .setResult(standardDeviationResult)
                        .build();

                responseObserver.onNext(response);
                LOGGER.debug("Send std result: - {}", standardDeviationResult);
                responseObserver.onCompleted();
            }

            private double[] getNumbersArray() {
                int size = numbers.size();
                double[] numbersArray = new double[size];
                for (int i = 0; i < size; i++) {
                    numbersArray[i] = numbers.get(i);
                }
                return numbersArray;
            }
        };

        return requestStreamObserver;
    }
}
