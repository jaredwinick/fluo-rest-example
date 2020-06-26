package com.github.jaredwinick;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Client {

    public class FindItemRunnable implements Runnable {

        private HttpClient httpClient;
        private Integer value;
        private AtomicLong counter;
        public FindItemRunnable(HttpClient httpClient, Integer value, AtomicLong counter) {
            this.httpClient = httpClient;
            this.value = value;
            this.counter = counter;
        }
        @Override
        public void run() {

            CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
            httpClient.request(HttpMethod.GET, "/item/find?value=" + value, response -> {
                log.info("Received response to GET with status code " + response.statusCode());
                response.bodyHandler(totalBuffer -> {
                    JsonArray jsonArray = totalBuffer.toJsonArray();
                    List<Item> itemList = jsonArray
                            .stream()
                            .map(JsonObject.class::cast)
                            .map(jsonObject -> jsonObject.mapTo(Item.class))
                            .collect(Collectors.toList());
                    log.info(String.format("GET count %d", counter.incrementAndGet()));

                    // Verify each Item received has the value we were searching for
                    for (Item item : itemList) {
                        if (item.getValue() != value) {
                            log.error(itemList.toString());
                            throw new RuntimeException("Value in Item does not match search value");
                        }
                    }

                    completableFuture.complete(response.statusCode());
                });
            }).end();

            // Block the thread to wait for the response. Doing this since we have a thread pool and just testing
            try {
                completableFuture.get();
            } catch (Exception e) {
                log.error("Error blocking on response", e);
            }
        }
    }

    public class UpdateItemRunnable implements Runnable {

        private HttpClient httpClient;
        private Integer value;
        private AtomicLong counter;
        private String id;
        public UpdateItemRunnable(HttpClient httpClient, String id, Integer value, AtomicLong counter) {
            this.httpClient = httpClient;
            this.id = id;
            this.value = value;
            this.counter = counter;
        }
        @Override
        public void run() {

            CompletableFuture<Integer> completableFuture = new CompletableFuture<>();
            httpClient.request(HttpMethod.PUT, "/item", response -> {
                log.info("Received response to PUT with status code " + response.statusCode());
                log.info(String.format("PUT count %d", counter.incrementAndGet()));
                completableFuture.complete(response.statusCode());

            }).putHeader("content-type", "application/json").end(Json.encode(new Item(id, value)));

            // Block the thread to wait for the response. Doing this since we have a thread pool and just testing
            try {
                completableFuture.get();
            } catch (Exception e) {
                log.error("Error blocking on response", e);
            }
        }
    }

    private static Logger log = LoggerFactory.getLogger(Client.class);

    public Client() {}

    private final Integer MAX_NUMBER_OF_VALUES = 100;
    private final Integer MAX_NUMBER_OF_IDS = 3;

    public void run(int serverPort) throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        HttpClientOptions options = new HttpClientOptions()
                .setKeepAlive(true)
                .setMaxPoolSize(10)
                .setDefaultHost("localhost")
                .setDefaultPort(serverPort);

        HttpClient client = vertx.createHttpClient(options);

        ExecutorService executor = Executors.newFixedThreadPool(20);

        AtomicLong findCounter = new AtomicLong();
        AtomicLong updateCounter = new AtomicLong();
        Random random = new Random();
        List<FindItemRunnable> runnables = new ArrayList<>();
        for (int i = 0; i < 10000; ++i) {
            executor.execute(new FindItemRunnable(client, random.nextInt(MAX_NUMBER_OF_VALUES), findCounter));
            executor.execute(new UpdateItemRunnable(client, String.format("%d", random.nextInt(MAX_NUMBER_OF_IDS)) , random.nextInt(MAX_NUMBER_OF_VALUES), updateCounter));
        }

        executor.shutdown();
        log.info("Waiting for executor termination");
        executor.awaitTermination(1000, TimeUnit.SECONDS);
        log.info("Executor terminated");
        vertx.close();
    }

    public static void main(String[] args) throws InterruptedException {

        Client client = new Client();
        client.run(8998);
    }
}
