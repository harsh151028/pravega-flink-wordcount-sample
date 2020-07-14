package com.harsh;

import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import com.harsh.flink.Utils;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class StreamingJobWriter {

    public static void main(String[] args) throws Exception {

        // initialize the parameter utility tool in order to retrieve input parameters
        ParameterTool params = ParameterTool.fromArgs(args);
        PravegaConfig pravegaConfig = PravegaConfig
                .fromParams(params)
                .withDefaultScope(Constants.DEFAULT_SCOPE);

        // create the Pravega input stream (if necessary)
        Stream stream = Utils.createStream(
                pravegaConfig,
                params.get(Constants.STREAM_PARAM, Constants.DEFAULT_STREAM));

        // retrieve the socket host and port information to read the incoming data from
        String host = params.get(Constants.HOST_PARAM, Constants.DEFAULT_HOST);
        int port = Integer.parseInt(params.get(Constants.PORT_PARAM, Constants.DEFAULT_PORT));

        // initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> dataStream = env.socketTextStream(host, port);

        // create the Pravega sink to write a stream of text
        FlinkPravegaWriter<String> writer = FlinkPravegaWriter.<String>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withEventRouter(new EventRouter())
                .withSerializationSchema(PravegaSerialization.serializationFor(String.class))
                .build();
        dataStream.addSink(writer).name("Pravega Stream");

        // create another output sink to print to stdout for verification
        dataStream.print().name("stdout");

        // execute within the Flink environment
        env.execute("WordCountWriter");
    }

    /*
     * Event Router class
     */
    public static class EventRouter implements PravegaEventRouter<String> {
        // Ordering - events with the same routing key will always be
        // read in the order they were written
        @Override
        public String getRoutingKey(String event) {
            return "SameRoutingKey";
        }
    }
}
