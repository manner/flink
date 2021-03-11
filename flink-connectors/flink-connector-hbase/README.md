# Flink HBase Connector

This module provides connectors that allow Flink to access [HBase](https://hbase.apache.org/) using [CDC](https://en.wikipedia.org/wiki/Change_data_capture) . 
It supports the new Source and Sink API specified in [FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) and [FLIP-143](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API).

## Installing HBase

Follow the instructions from the [HBase Quick Start Guide](http://hbase.apache.org/book.html#quickstart) to install HBase.

*Version Compatibility*: This module is compatible with Apache HBase *2.3.4*.

## HBase Configuration

Connecting to HBase always requires a `Configuration` instance.
If there is an HBase gateway on the same host as the Flink gateway where the application is started, this can be obtained by invoking `HBaseConfiguration.create()` as in the examples below.
If that's not the case a configuration should be provided where the proper core-site, hdfs-site, and hbase-site are added as resources.

## DataStream API

### Reading data from HBase

To receive data from HBase, the connector makes use of the internal replication mechanism of HBase. 
The connector registers at the HBase cluster as a *Replication Peer* and will receive all change events from HBase.

For the replication to work, the HBase config needs to have replication enabled in the `hbase-site.xml` file.
This needs be done only once per cluster:
```xml
<configuration>
  <property>
    <name>hbase.replication</name>
    <value>true</value>
  </property>
  ...
</configuration>
```
All incoming events to Flink will be processed as an `HBaseEvent`. 
You will need to specify a Deserializer which will transform each event from an `HBaseEvent` to the desired DataStream type.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Configuration hbaseConfig = HBaseConfiguration.create();
hbaseConfig.setBoolean("hbase.replication", true);
String tableName = "TestTable";

HBaseSource<String> hbaseSource =
    HBaseSource.<String>builder()
        .setTableName(tableName)
        .setSourceDeserializer(new HBaseStringDeserializer())
        .setHBaseConfiguration(hbaseConfig)
        .build();

DataStream<String> stream = env.fromSource(
        hbaseSource,
        WatermarkStrategy.noWatermarks(),
        "HBaseSource");
// ...
```

The Deserializer is created as follows:

```java
static class HBaseStringDeserializer extends HBaseSourceDeserializer<String> {
    @Override
    public String deserialize(HBaseEvent event) {
        return new String(event.getPayload());
    }
}
```

### Writing data to HBase
To write data from Flink into HBase, you can use the `HBaseSink`.
Similar to the `HBaseSource` you need to specify a Serializer which knows how to write your DataStream element into HBase.

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Configuration hbaseConfig = HBaseConfiguration.create();
String tableName = "TestTable";

DataStream<Long> longStream = env.fromSequence(0, 100);

HBaseSink<Long> hbaseSink =
    HBaseSink.<Long>builder()
        .setTableName(tableName)
        .setSinkSerializer(new HBaseLongSerializer())
        .setHBaseConfiguration(hbaseConfig)
        .build();

longStream.sinkTo(hbaseSink);
// ...
```
An example Serializer is given below. You need to implement the following five methods, so the connector 
knows how to save the data to HBase.
```java
static class HBaseLongSerializer implements HBaseSinkSerializer<Long> {
    @Override
    public byte[] serializePayload(Long event) {
        // This is the actual value written to HBase.
        return Bytes.toBytes(event);
    }

    @Override
    public byte[] serializeColumnFamily(Long event) {
        return Bytes.toBytes("exampleColumnFamily");
    }

    @Override
    public byte[] serializeQualifier(Long event) {
        return Bytes.toBytes("exampleQualifier");
    }

    @Override
    public byte[] serializeRowKey(Long event) {
        return Bytes.toBytes(event.toString());
    }

    @Override
    public Class<? extends Mutation> serializeRowType(Long event) {
        // You can also send deletion events with Delete.class 
        return Put.class;
    }
}
```

## Building the connector

Note that the streaming connectors are not part of the binary distribution of Flink.
You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-stable/dev/project-configuration.html#adding-connector-and-library-dependencies).

The connector can be built by using maven:

```
cd flink-connectors/flink-connector-hbase
mvn clean install
```
