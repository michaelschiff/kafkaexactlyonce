maven_jar(
    name = "org_apache_curator_curator_test",
    artifact = "org.apache.curator:curator-test:2.8.0",
)

maven_jar(
    name = "org_apache_curator_curator_framework",
    artifact = "org.apache.curator:curator-framework:2.8.0"
)

maven_jar(
    name = "org_apache_curator_curator_client",
    artifact = "org.apache.curator:curator-client:2.8.0"
)

maven_jar(
    name = "org_apache_kafka_kafka_2_11",
    artifact = "org.apache.kafka:kafka_2.11:0.9.0.1"
)

maven_jar(
    name = "org_apache_kafka_kafka_clients",
    artifact = "org.apache.kafka:kafka-clients:0.9.0.1"
)

maven_jar(
    name = "net_sf_jopt_simple_jopt_simple",
    artifact = "net.sf.jopt-simple:jopt-simple:5.0.4"
)

maven_jar(
    name = "com_google_guava_guava",
    artifact = "com.google.guava:guava:19.0"
)

maven_jar(
    name = "org_slf4j_slf4j_api",
    artifact = "org.slf4j:slf4j-api:1.7.25"
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_core",
    artifact = "com.fasterxml.jackson.core:jackson-core:2.9.5"
)

maven_jar(
    name = "com_fasterxml_jackson_core_jackson_databind",
    artifact = "com.fasterxml.jackson.core:jackson-databind:2.9.5"
)

maven_jar(
    name = "org_apache_zookeeper_zookeeper",
    artifact = "org.apache.zookeeper:zookeeper:3.4.5"
)

maven_jar(
    name = "com_google_protobuf_protobuf_java",
    artifact = "com.google.protobuf:protobuf-java:3.5.1"
)

maven_jar(
    name = "org_javassist_javassist",
    artifact = "org.javassist:javassist:3.18.1-GA"
)

maven_jar(
    name = "org_scala_lang_scala_library",
    artifact = "org.scala-lang:scala-library:2.11.12"
)

maven_jar(
    name = "com_yammer_metrics_metrics_core",
    artifact = "com.yammer.metrics:metrics-core:2.1.5"
)

# proto_library, cc_proto_library, and java_proto_library rules implicitly
# depend on @com_google_protobuf for protoc and proto runtimes.
# This statement defines the @com_google_protobuf repo.
http_archive(
    name = "com_google_protobuf",
    sha256 = "cef7f1b5a7c5fba672bec2a319246e8feba471f04dcebfe362d55930ee7c1c30",
    strip_prefix = "protobuf-3.5.0",
    urls = ["https://github.com/google/protobuf/archive/v3.5.0.zip"],
)

# java_lite_proto_library rules implicitly depend on @com_google_protobuf_javalite//:javalite_toolchain,
# which is the JavaLite proto runtime (base classes and common utilities).
http_archive(
    name = "com_google_protobuf_javalite",
    sha256 = "d8a2fed3708781196f92e1e7e7e713cf66804bd2944894401057214aff4f468e",
    strip_prefix = "protobuf-5e8916e881c573c5d83980197a6f783c132d4276",
    urls = ["https://github.com/google/protobuf/archive/5e8916e881c573c5d83980197a6f783c132d4276.zip"],
)