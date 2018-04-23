java_library(
    name = "consumer",
    srcs = glob(["*.java"]),
    deps = [
        "//src/main/proto:kafkaexactlyonce_java_proto",
        "@org_apache_kafka_kafka_clients//jar",
        "@org_apache_kafka_kafka_2_11//jar",
        "@org_slf4j_slf4j_api//jar",
        "@org_apache_zookeeper_zookeeper//jar",
        "@org_apache_curator_curator_framework//jar",
        "@org_apache_curator_curator_client//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
    ]
)