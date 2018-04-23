java_library(
    name = "producer",
    srcs = glob(["*.java"]),
    deps = [
        "//src/main/proto:kafkaexactlyonce_java_proto",
        "@org_apache_kafka_kafka_clients//jar",
        "@org_apache_kafka_kafka_2_11//jar",
        "@org_slf4j_slf4j_api//jar",
    ]
)