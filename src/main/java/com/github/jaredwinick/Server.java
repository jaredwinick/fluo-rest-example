package com.github.jaredwinick;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.PrometheusScrapingHandler;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.mini.MiniFluo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class Server {

    private static Logger log = LoggerFactory.getLogger(Server.class);

    private final FluoClient fluoClient;
    private final Column itemRecordColumn;
    private final String itemRowPrefix = "item";
    private final String indexRowPrefix = "index";

    public Server(FluoConfiguration fluoConfiguration) {
        MiniFluo miniFluo = FluoFactory.newMiniFluo(fluoConfiguration);
        fluoClient = FluoFactory.newClient(miniFluo.getClientConfiguration());
        itemRecordColumn = new Column("item", "json");
    }

    private String getItemRow(String id) {
        return itemRowPrefix + id;
    }

    private String getIndexRow(String value) {
        return indexRowPrefix + value;
    }

    /**
     * This will handle both POSTs and PUTs because for this example
     * we don't really care about checking if an Item already exists or not
     * @param routingContext
     */
    private void createUpdateItemHandler(RoutingContext routingContext) {
        log.info("In createUpdateItemHandler" + routingContext.getBodyAsJson());
        Boolean commitSuccessful = true;
        Item item = null;
        try {
            item = routingContext.getBodyAsJson().mapTo(Item.class);
        }
        catch (Throwable t) {
            log.error(t.getMessage(), t);
        }

        log.info("Got item:" + item.toString());

        try (Transaction tx = fluoClient.newTransaction()) {

            // look up existing item so we can delete previous index values if they exist
            String existingItemString = tx.gets(getItemRow(item.getId()), itemRecordColumn);
            Item existingItem = null;
            if (existingItemString != null) {
                existingItem = Json.decodeValue(existingItemString, Item.class);
                tx.delete(getIndexRow(existingItem.getValue().toString()), new Column("id", existingItem.getId()));
            }

            // set a entry for the serialized Item as well as an index by its value
            // Avoid an AlreadySetException by not setting if there is no change in value
            if (existingItem == null || (existingItem != null && existingItem.getValue() != item.getValue())) {
                tx.set(getItemRow(item.getId()), itemRecordColumn, Json.encode(item));
                tx.set(getIndexRow(item.getValue().toString()), new Column("id", item.getId()), "");
            }
            try {
                tx.commit();
                log.info("Successful commit for item {}", item.getId());
            }
            catch (Exception commitException) {
                log.warn("Commit for item {} failed", item.getId());
                commitSuccessful = false;
            }
        }

        HttpServerResponse response = routingContext.response();
        response.putHeader("content-type", "application/json");
        if (commitSuccessful) {
            response.setStatusCode(200).end(Json.encode(item));
        }
        else {
            response.setStatusCode(500).end("Unable to commit Item");
        }
    }

    private void findItemHandler(RoutingContext routingContext) {

        String value = routingContext.request().getParam("value");
        log.info("Searching for value: {}", value);

        List<Item> itemList = findItemsByValue(value);
        routingContext
                .response()
                .putHeader("content-type", "application/json")
                .setStatusCode(200)
                .end(Json.encode(itemList));
    }

    /**
     * Scans the index rows for the value and then fetches any
     * items based on the referenced item ids
     * @param value search value
     * @return a List of zero or more Items
     */
    private List<Item> findItemsByValue(String value) {

        List<Item> itemList;
        try (Snapshot snapshot = fluoClient.newSnapshot()) {

            CellScanner cellScanner = snapshot.scanner().over(getIndexRow(value)).build();
            List<String> itemRowList =
                    cellScanner.stream()
                            .map(rcv -> rcv.getColumn().getsQualifier())
                            .map(itemId -> getItemRow(itemId))
                            .collect(Collectors.toList());
            itemList = snapshot
                    .gets(itemRowList, itemRecordColumn)
                    .values()
                    .stream()
                    .map(columnStringMap -> columnStringMap.get(itemRecordColumn))
                    .map(itemJson -> Json.decodeValue(itemJson, Item.class))
                    .collect(Collectors.toList());

        }
        return itemList;
    }

    public void run(Integer port) {

        Vertx vertx = Vertx.vertx(new VertxOptions().setMetricsOptions(
                new MicrometerMetricsOptions()
                        .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                        .setEnabled(true)));
        Router router = Router.router(vertx);
        HttpServer server = vertx.createHttpServer();

        router.route().handler(BodyHandler.create());
        router.put("/item").handler(this::createUpdateItemHandler);
        router.post("/item").handler(this::createUpdateItemHandler);
        router.get("/item/find").handler(this::findItemHandler);
        router.route("/metrics").handler(PrometheusScrapingHandler.create());

        log.info("Server ready to handle requests");
        server.requestHandler(router).listen(port);
    }

    public static void main(String[] args) throws IOException {

        String tmpDir = Files.createTempDirectory(Paths.get("target"), "mini").toString();

        FluoConfiguration fluoConfig = new FluoConfiguration();
        fluoConfig.setApplicationName("class");
        fluoConfig.setMiniDataDir(tmpDir);

        log.info("Starting MiniFluo ... ");
        Server server = new Server(fluoConfig);
        server.run(8998);
    }
}
