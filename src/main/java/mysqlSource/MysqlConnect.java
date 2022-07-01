package mysqlSource;

import com.linkall.vance.common.env.EnvUtil;
import io.cloudevents.CloudEvent;
import io.cloudevents.jackson.JsonFormat;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventData;

import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.message.MessageReader;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;

import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;

public class MysqlConnect {
    private static String database;
    private static String table;
    private static String rowData = "";
    private static String finalData = "";

    public static void main(String[] args) throws IOException {

        final Vertx vertx = Vertx.vertx();
        final WebClient webClient = WebClient.create(vertx);
        final String v_target = "http://localhost:8080";
        //binlog connect
        BinaryLogClient client = new BinaryLogClient("localhost", 3306, "root", "Michaelg22");
        client.registerEventListener(event -> {
            EventData data = event.getData();

            if (null != data) {
                String fon = null;
                String eventData = data.toString();
                String splitStr = eventData;


                //get database and table from the binlog
                String tableRow = "table=";
                boolean tableTrue = splitStr.contains(tableRow);
                if (tableTrue) {
                    String[] strArray = splitStr.split(",");
                    String x = strArray[1];
                    String y = strArray[2];
                    String z = x + y;
                    String[] zArray = z.split("'");
                    database = zArray[1];
                    table = zArray[3];
                }

                //handle the 3 different types of events
                //**************************************
                //Write event
                String WriteRows = "WriteRowsEventData";
                boolean WriteTrue = eventData.contains(WriteRows);
                if (WriteTrue) {
                    fon = "write";
                    //Get data
                    String rows = "rows=";
                    boolean rowsTrue = splitStr.contains(rows);
                    if (rowsTrue) {
                        String[] strArray = splitStr.split("=");
                        String x = strArray[3];
                        String[] xArray = x.split("\n");
                        int numEntry = xArray.length;
                        //remove extra rows, first and last row
                        numEntry = numEntry - 2;
                        //Count the amount of entries and add them in one string.
                        String allData = "";
                        int numArray = 1;
                        for (int i = 1; i <= numEntry; i++) {
                            String y = xArray[numArray];
                            allData = allData + y;
                            numArray++;
                        }
                        //split the data again and change it in the correct format for mysql.
                        String[] dataArray = allData.split("[\\[|\\]]");
                        int numData = 0;
                        for (int i = 1; i <= numEntry; i++) {
                            numData++;
                            if (numData <= numEntry - 1) {
                                String y = dataArray[numData];
                                rowData = rowData + "(" + y + "), ";
                            } else {
                                String y = dataArray[numData];
                                rowData = rowData + "(" + y + ")";
                            }
                            numData++;

                        }
                    }
                }
                //Update event not functioning
                String UpdateRows = "UpdateRowsEventData";
                boolean UpdateTrue = eventData.contains(UpdateRows);
                if (UpdateTrue) {
                    fon = "update";
                    System.out.println(eventData);

                    String rows = "rows=";
                    boolean rowsTrue = splitStr.contains(rows);
                    if (rowsTrue) {
                        String[] strArray = splitStr.split("=");
                        //need to be continued
                    }
                }
                //delete event npt functioning...
                String DelRows = "DeleteRowsEventData";
                boolean DelTrue = eventData.contains(DelRows);
                if (DelTrue)
                    fon = "delete";
                // To be continued

                finalData = fon + "@" + database + "@" + table + "@" + rowData;

                if (null != fon) {
                    System.out.println(finalData);


                    // Sent to http server

                }

                    // Build CloudEvent
                    CloudEvent ce = buildEvent(finalData);
                    // Sent to http server
                    Future<HttpResponse<Buffer>> responseFuture =
                            VertxMessageFactory.createWriter(webClient.postAbs(v_target))
                                    .writeStructured(ce, JsonFormat.CONTENT_TYPE);
                    responseFuture
                            .map(VertxMessageFactory::createReader) // Let's convert the response to message reader...
                            .map(MessageReader::toEvent) // ...then to event
                            .onSuccess(System.out::println) // Print the received message
                            .onFailure(System.err::println); // Print the eventual failure



            }


        });
        client.connect();
    }

    private static CloudEvent buildEvent(String eventData){


        CloudEventBuilder eventTemplate = CloudEventBuilder.v1();

        eventTemplate.withDataContentType("application/json")
                .withTime(OffsetDateTime.now()).withType("http")
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("mysql-binlog"));

        JsonObject dataJson = new JsonObject();
        dataJson.put("data", eventData);
        eventTemplate.withData(dataJson.toBuffer().getBytes());

        return eventTemplate.build();

    }
}