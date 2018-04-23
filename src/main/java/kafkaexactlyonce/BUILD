java_binary(
    name = "kafkaexactlyonce",
    main_class = "kafkaexactlyonce.KafkaExactlyOnce",
    srcs = glob(["*.java"]),
    deps = [
        "//src/main/proto:kafkaexactlyonce_java_proto",
        "//src/main/java/kafkaexactlyonce/consumer",
        "//src/main/java/kafkaexactlyonce/producer",
        "//src/main/java/kafkaexactlyonce/utils",
        "@org_apache_kafka_kafka_clients//jar",
        "@org_apache_kafka_kafka_2_11//jar",
        "@org_slf4j_slf4j_api//jar",
        "@org_apache_zookeeper_zookeeper//jar",
        "@org_apache_curator_curator_framework//jar",
        "@org_apache_curator_curator_client//jar",
        "@org_apache_curator_curator_test//jar",
        "@com_fasterxml_jackson_core_jackson_databind//jar",
        "@com_fasterxml_jackson_core_jackson_core//jar",
        "@org_javassist_javassist//jar",
        "@com_google_guava_guava//jar",
        "@org_scala_lang_scala_library//jar",
        "@com_yammer_metrics_metrics_core//jar",
    ]
)