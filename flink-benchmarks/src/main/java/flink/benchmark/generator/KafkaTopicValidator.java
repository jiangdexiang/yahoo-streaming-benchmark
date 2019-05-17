package flink.benchmark.generator;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

/**
 * Simple util to print kafka partitions locally
 */
public class KafkaTopicValidator {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);
        DataStream<String> rawMessageStream = env.addSource(new FlinkKafkaConsumer010<>(
                parameterTool.getRequired("kafka.topic"),
                new SimpleStringSchema(),
                parameterTool.getProperties()));

        rawMessageStream.print();

        env.execute();
    }
}
