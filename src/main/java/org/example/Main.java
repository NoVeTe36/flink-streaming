package org.example;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Os {
        @JsonProperty("platform")
        public String platform;
        @JsonProperty("name")
        public String name;
        @JsonProperty("version")
        public String version;
        @JsonProperty("codename")
        public String codename;
        @JsonProperty("type")
        public String type;
        @JsonProperty("family")
        public String family;
        @JsonProperty("kernel")
        public String kernel;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Host {
        @JsonProperty("name")
        public String name;
        @JsonProperty("id")
        public String id;
        @JsonProperty("hostname")
        public String hostname;
        @JsonProperty("mac")
        public String[] mac;  // Ensure this is an array or list for the SQL setArray
        @JsonProperty("architecture")
        public String architecture;
        @JsonProperty("containerized")
        public boolean containerized;
        @JsonProperty("ip")
        public String[] ip;
        @JsonProperty("os")
        public Os os;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class File {
        @JsonProperty("path")
        public String path;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Log {
        @JsonProperty("file")
        public File file;
        @JsonProperty("offset")
        public long offset;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LogEntry {
        @JsonProperty("error")
        public String error;
        @JsonProperty("level")
        public String level;
        @JsonProperty("function")
        public String function;
        @JsonProperty("respondTime")
        public int respondTime;
        @JsonProperty("node_ip")
        public String node_ip;
        @JsonProperty("@timestamp")
        public String timestamp;
        @JsonProperty("masterAccount")
        public String masterAccount;
        @JsonProperty("resultCode")
        public String resultCode;
        @JsonProperty("resultValue")
        public String resultValue;
        @JsonProperty("message")
        public String message;
        @JsonProperty("thread")
        public String thread;
        @JsonProperty("channel")
        public String channel;
        @JsonProperty("class")
        public String className;
        @JsonProperty("requestId")
        public String requestId;
        @JsonProperty("sourceapp")
        public String sourceapp;
        @JsonProperty("inputs")
        public String inputs;
        @JsonProperty("host")
        public Host host;
        @JsonProperty("log")
        public Log log;
    }

    public static class JSONValueDeserializationSchema implements DeserializationSchema<JSONObject> {
        @Override
        public JSONObject deserialize(byte[] message) {
            return JSONObject.parseObject(new String(message));
        }

        @Override
        public boolean isEndOfStream(JSONObject nextElement) {
            return false;
        }

        @Override
        public TypeInformation<JSONObject> getProducedType() {
            return TypeInformation.of(JSONObject.class);
        }
    }

    public static class ExtractLogEntryFunction implements MapFunction<JSONObject, JSONObject> {
        @Override
        public JSONObject map(JSONObject value) throws Exception {
            return value;
        }
    }

    public static class ExtractLogFunction implements MapFunction<JSONObject, JSONObject> {
        @Override
        public JSONObject map(JSONObject value) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            // Extract log and file from the JSON object
            JsonNode logNode = mapper.readTree(value.getString("log"));
            JsonNode fileNode = mapper.readTree(logNode.get("file").toString());
            File file = mapper.treeToValue(fileNode, File.class);
            JSONObject fileJson = new JSONObject();
            fileJson.put("path", file.path);
            Log log = mapper.treeToValue(logNode, Log.class);
            JSONObject logJson = new JSONObject();
            logJson.put("file", fileJson);
            logJson.put("offset", log.offset);

            return logJson;
        }
    }

    public static class ExtractHostFunction implements MapFunction<JSONObject, JSONObject> {
        @Override
        public JSONObject map(JSONObject value) throws Exception {
//            return value.getJSONObject("host");
            ObjectMapper mapper = new ObjectMapper();

            // Extract host and os from the JSON object
            JsonNode jsonNode = mapper.readTree(value.getString("host"));
            JsonNode osNode = mapper.readTree(jsonNode.get("os").toString());
            Os os = mapper.treeToValue(osNode, Os.class);
            JSONObject osJson = new JSONObject();
            osJson.put("platform", os.platform);
            osJson.put("name", os.name);
            osJson.put("version", os.version);
            osJson.put("codename", os.codename);
            osJson.put("type", os.type);
            osJson.put("family", os.family);
            osJson.put("kernel", os.kernel);

            Host host = mapper.treeToValue(jsonNode, Host.class);
            JSONObject hostJson = new JSONObject();
            hostJson.put("name", host.name);
            hostJson.put("id", host.id);
            hostJson.put("host_name", host.hostname);
            hostJson.put("mac", host.mac);
            hostJson.put("architecture", host.architecture);
            hostJson.put("containerized", host.containerized);
            hostJson.put("ip", host.ip);
            hostJson.put("os", osJson);

            return hostJson;
        }
    }

    public static class CustomJdbcStatementBuilder implements JdbcStatementBuilder<JSONObject> {
        @Override
        public void accept(java.sql.PreparedStatement ps, JSONObject value) throws java.sql.SQLException {
            ps.setString(1, value.getString("id"));
            ps.setString(2, value.getString("name"));

            String[] macArray = value.getObject("mac", String[].class);
            java.sql.Array macSqlArray = ps.getConnection().createArrayOf("text", macArray);
            ps.setArray(3, macSqlArray);

            ps.setString(4, value.getString("architecture"));
            ps.setBoolean(5, value.getBoolean("containerized"));

            String[] ipArray = value.getObject("ip", String[].class);
            java.sql.Array ipSqlArray = ps.getConnection().createArrayOf("text", ipArray);
            ps.setArray(6, ipSqlArray);

            JSONObject os = value.getJSONObject("os");
            ps.setString(7, os.getString("platform"));
            ps.setString(8, os.getString("name"));
            ps.setString(9, os.getString("version"));
            ps.setString(10, os.getString("codename"));
            ps.setString(11, os.getString("type"));
            ps.setString(12, os.getString("family"));
            ps.setString(13, os.getString("kernel"));
        }
    }

    //    process to count the number of logs and write to a file
    public static class CountLogsFunction implements MapFunction<JSONObject, JSONObject> {
        private static int count = 0;

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            count++;
            JSONObject result = new JSONObject();
            result.put("count", count);
            return result;
        }
    }

    public static class MasterAccount {
        public static JSONObject masterAccountStore = new JSONObject();
    }

    public static class CountLogsByMasterAccountFunction implements MapFunction<JSONObject, JSONObject> {
        @Override
        public JSONObject map(JSONObject value) throws Exception {
            String masterAccount = value.getString("masterAccount");
            String timestamp = value.getString("@timestamp");

            if (MasterAccount.masterAccountStore.containsKey(masterAccount)) {
                JSONObject accountData = MasterAccount.masterAccountStore.getJSONObject(masterAccount);

                int count = accountData.getIntValue("Number of logs");
                accountData.put("Number of logs", count + 1);

                JSONArray timestampArray = accountData.getJSONArray("Timestamp");
                timestampArray.add(timestamp);
                accountData.put("Timestamp", timestampArray);
            } else {
                JSONObject accountData = new JSONObject();
                accountData.put("Number of logs", 1);

                JSONArray timestampArray = new JSONArray();
                timestampArray.add(timestamp);
                accountData.put("Timestamp", timestampArray);

                MasterAccount.masterAccountStore.put(masterAccount, accountData);
            }

            return MasterAccount.masterAccountStore;
        }
    }

    /**
     * Count active users in 30-minute time slots
     */

    public static class ActiveUser {
        public static JSONObject activeUserStore = new JSONObject();
    }

    public static class CountActiveUsersFunction implements MapFunction<JSONObject, JSONObject> {

        @Override
        public JSONObject map(JSONObject value) throws Exception {
            String userId = value.getString("masterAccount");
            String timestamp = value.getString("@timestamp");

            // Convert timestamp to 30-minute slot
            String timeSlot = getTimeSlot(timestamp);

            // Check if the time slot already exists in the store
            if (ActiveUser.activeUserStore.containsKey(timeSlot)) {
                JSONObject slotData = ActiveUser.activeUserStore.getJSONObject(timeSlot);

                // Get current active users in this slot
                JSONArray activeUsers = slotData.getJSONArray("Active Users");

                // If the user is not already in the array, add them
                if (!activeUsers.contains(userId)) {
                    activeUsers.add(userId);
                }

                // Update number of active users
                slotData.put("Number of active users", activeUsers.size());
                slotData.put("Active Users", activeUsers);

            } else {
                // Create new time slot if it does not exist
                JSONObject slotData = new JSONObject();
                JSONArray activeUsers = new JSONArray();
                activeUsers.add(userId);
                slotData.put("Number of active users", 1);
                slotData.put("Active Users", activeUsers);
                ActiveUser.activeUserStore.put(timeSlot, slotData);
            }

            return ActiveUser.activeUserStore;
        }

        public static String getTimeSlot(String timestamp) throws Exception {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            Date date = dateFormat.parse(timestamp);
            SimpleDateFormat hourMinuteFormat = new SimpleDateFormat("HH:mm");

            // Extract hour and minute
            String time = hourMinuteFormat.format(date);
            String[] timeParts = time.split(":");
            int hour = Integer.parseInt(timeParts[0]);
            int minute = Integer.parseInt(timeParts[1]);

            // Determine the 30-minute slot
            if (minute < 30) {
                return String.format("%02d:00 - %02d:30", hour, hour);
            } else {
                return String.format("%02d:30 - %02d:00", hour, (hour + 1) % 24);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "test-source-topic";
        String brokers = "172.16.3.191:9092";

        KafkaSource<JSONObject> source = KafkaSource.<JSONObject>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        DataStream<JSONObject> stream = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source");

        DataStream<JSONObject> hostStream = stream.map(new ExtractHostFunction());

        DataStream<JSONObject> logStream = stream.map(new ExtractLogFunction());

        DataStream<JSONObject> logEntryStream = stream.map(new ExtractLogEntryFunction());

        hostStream.writeAsText("file:///home/ioit/data-flink/host.txt").setParallelism(1); // Ensure only one writer for output

        logStream.writeAsText("file:///home/ioit/data-flink/log.txt").setParallelism(1); // Ensure only one writer for output

        logEntryStream.writeAsText("file:///home/ioit/data-flink/log_entry.txt").setParallelism(1); // Ensure only one writer for output

        hostStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO host (id, name, mac, architecture, containerized, ip, os_platform, os_name, os_version, os_codename, os_type, os_family, os_kernel) " +
                                "VALUES (?, ? , ?, ? , ?, ? , ?, ? , ?, ? , ?, ? , ?)",
                        new CustomJdbcStatementBuilder(),  // Use the custom statement builder
                        new JdbcExecutionOptions.Builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:postgresql://10.42.66.254:5432/flink-database")
                                .withDriverName("org.postgresql.Driver")
                                .withUsername("ioit")
                                .withPassword("Admin_2024")
                                .build()
                )
        );

        stream.map(new CountLogsFunction())
                .writeAsText("file:///home/ioit/data-flink/count.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
                .setParallelism(3);

        stream.map(new CountLogsByMasterAccountFunction())
                .writeAsText("file:///home/ioit/data-flink/count_by_master_account.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
                .setParallelism(3);

        stream.map(new CountActiveUsersFunction())
                .writeAsText("file:///home/ioit/data-flink/active_user.txt", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)
                .setParallelism(3);

        env.execute("Flink Kafka to PostgreSQL");
    }
}
