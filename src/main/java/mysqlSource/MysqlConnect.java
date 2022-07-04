package mysqlSource;

import io.cloudevents.CloudEvent;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventData;

import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.http.vertx.VertxMessageFactory;
import io.cloudevents.jackson.JsonFormat;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;


import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;

public class MysqlConnect {


    private static String database = "";
    private static  String table = "";

    public static void main(String[] args) throws IOException, ParseException {

        final Vertx vertx = Vertx.vertx();
        final WebClient webClient = WebClient.create(vertx);
        final String v_target = "http://localhost:8080";


        //Config
        String v_config = "src/main/resources/config.json";
        Object obj = new JSONParser().parse(new FileReader(v_config));

        // typecasting obj to JSONObject
        JSONObject jo = (JSONObject) obj;

        // getting login for Binlog
        String address = (String) jo.get("address");
        String X = (String) jo.get("sqlPort");
        int sqlPort = Integer.parseInt(X);
        String username = (String) jo.get("username");
        String password = (String) jo.get("password");

        //binlog connect
        BinaryLogClient client = new BinaryLogClient(address, sqlPort, username, password);
        client.registerEventListener(event -> {
            EventData data = event.getData();
            //System.out.println(data);
            if (null != data) {
                String fun = null;

                String rowData = "";
                String eventData = data.toString();
                String splitStr = eventData;
                String finalData;

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
                //************************************************
                //Write event
                String WriteRows = "WriteRowsEventData";
                boolean WriteTrue = eventData.contains(WriteRows);
                if (WriteTrue) {
                    fun = "write";
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
                        //remove the extra special character.
                        String[] dataArray = allData.toString().split("[\\[|\\]]");
                        int numData = 0;
                        for (int i = 1; i <= numEntry; i++) {
                            numData++;

                            String y = dataArray[numData];
                            rowData = rowData + y + "*";

                            numData++;

                        }


                    }
                }

                //Update event
                String UpdateRows = "UpdateRowsEventData";
                boolean UpdateTrue = eventData.contains(UpdateRows);
                if (UpdateTrue) {
                    fun = "update";
                    boolean rowsTrue = splitStr.contains("rows=");
                    if (rowsTrue) {

                        String[] strArray = splitStr.split("=");
                        String x = strArray[5] + strArray[6];


                        int numEntry = strArray.length;
                        //remove extra rows
                        numEntry = numEntry - 5;
                        numEntry = numEntry /2;

                        //remove the extra special character.
                        String[] dataArray = x.split("[\\[|\\]]");
                        int numData = 0;
                        /*
                        <-------need to be changed ------->
                         */
                        System.out.println(numEntry);
                        for (int i = 0; i <= numEntry; i++) {
                            numData++;

                            String y = dataArray[numData];
                            rowData = rowData + y + "*";

                            numData++;

                        }
                        /*
                        Need to transform the data correctly. separate with before and after change to fit in
                        the js model lower. fit before and after data I need to skip one example
                        *col 1 = 0:2 col 2 = 1:3* after repeat the process for each entry,
                        before. before col1 after col1, before col2 after col2*...
                         */

                    }

                }


                //delete event
                String DelRows = "DeleteRowsEventData";
                boolean DelTrue = eventData.contains(DelRows);
                if (DelTrue) {
                    fun = "delete";

                    boolean rowsTrue = splitStr.contains("rows=");
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
                        //remove the extra special character.
                        String[] dataArray = allData.toString().split("[\\[|\\]]");
                        int numData = 0;
                        for (int i = 1; i <= numEntry; i++) {
                            numData++;

                            String y = dataArray[numData];
                            rowData = rowData + y + "*";

                            numData++;

                        }


                    }
                }

                if(null != fun){
                    //transform data into a jsObject
                    String colName; // col1, col2, ect...
                    String[] cols = rowData.split("\\*");
                    int entryNum = cols.length;

                    JSONObject jsData = new JSONObject();

                    for (int i = 0; i <= entryNum - 1; i++)// prep each entry to be sent as a separate jsObject
                    {
                        jsData.put("Function", fun);
                        jsData.put("Database", database);
                        jsData.put("Table", table);
                        int n = 0;
                        String[] singular = cols[i].split(",");
                        for (int y = 0; y <= singular.length - 1; y++) // place each col in the jsObject
                        {
                            n++;
                            colName = "col" + Integer.toString(n);
                            jsData.put(colName, singular[y]);
                        }
                        System.out.println(jsData);
                        // Sent to http server
                        CloudEvent ce = buildEvent(jsData);
                        Future<HttpResponse<Buffer>> responseFuture =
                                VertxMessageFactory.createWriter(webClient.postAbs(v_target))
                                        .writeStructured(ce, JsonFormat.CONTENT_TYPE);
                        jsData.clear();// clear the jsObject to send the next entry.
                    }

                }

            }


        });
        client.connect();
    }

    private static CloudEvent buildEvent(JSONObject eventData){
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