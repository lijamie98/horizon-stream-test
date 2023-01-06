package org.example;

import org.stellar.sdk.Server;
import org.stellar.sdk.requests.EventListener;
import org.stellar.sdk.requests.PaymentsRequestBuilder;
import org.stellar.sdk.requests.RequestBuilder;
import org.stellar.sdk.requests.SSEStream;
import org.stellar.sdk.responses.Page;
import org.stellar.sdk.responses.operations.OperationResponse;
import shadow.com.google.common.base.Optional;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class Main {
    static Server server = new Server("https://horizon.stellar.org");
    static Instant lastReceived = Instant.now();
    static Duration maxDuration = Duration.ZERO;


    static SSEStream<OperationResponse> startSSEStream() {
        String latestCursor = fetchLatestCursor();
        debugF("SSEStream last cursor=%s", latestCursor);

        PaymentsRequestBuilder paymentsRequest = server.payments().includeTransactions(true).cursor(latestCursor).order(RequestBuilder.Order.ASC).limit(200);
        return paymentsRequest.stream(new EventListener<>() {
            @Override
            public void onEvent(OperationResponse operationResponse) {
                debugF("received event %s", Long.toString(operationResponse.getId()));
                Instant now = Instant.now();
                Duration thisDuration = Duration.between(lastReceived, now);
                maxDuration = thisDuration.compareTo(maxDuration) >= 0 ? thisDuration : maxDuration;
                System.out.printf("since last received: %d seconds (max seconds:%d)%n", thisDuration.getSeconds(), maxDuration.getSeconds());
                lastReceived = now;
            }

            @Override
            public void onFailure(Optional<Throwable> exception, Optional<Integer> statusCode) {
                exception.get().printStackTrace();
            }
        });
    }

    static void debugF(String s, String latestCursor) {
        System.out.println(String.format(s, latestCursor));
    }

    static String fetchLatestCursor() {
        Page<OperationResponse> pageOpResponse;
        try {
            pageOpResponse = server.payments().order(RequestBuilder.Order.DESC).limit(200).execute();
        } catch (IOException e) {
            debugF("Error fetching the latest /payments result.", e.toString());
            return null;
        }

        if (pageOpResponse == null || pageOpResponse.getRecords() == null || pageOpResponse.getRecords().size() == 0) {
            return null;
        }
        return pageOpResponse.getRecords().get(0).getPagingToken();
    }

    public static void main(String[] args) {
        try {
            SSEStream<OperationResponse> response = startSSEStream();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}