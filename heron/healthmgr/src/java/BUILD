licenses(["notice"])

package(default_visibility = ["//visibility:public"])

healthmgr_deps_files = [
    "//heron/spi/src/java:common-spi-java",
    "//heron/spi/src/java:healthmgr-spi-java",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:common-java",
    "//heron/spi/src/java:metricsmgr-spi-java",
    "//heron/proto:proto_common_java",
    "//heron/proto:proto_metrics_java",
    "//heron/proto:proto_tmaster_java",
    "//heron/metricsmgr/src/thrift:thrift_scribe_java",
    "//third_party/java:jackson",
    "@org_yaml_snakeyaml//jar",
    "@com_google_protobuf_protobuf_java//jar",
    "@org_apache_thrift_libthrift//jar",
    "//third_party/java:logging", # slf4j is only used for thrift in scribe-sink
    "@javax_ws_rs_javax_ws_rs_api//jar",
    "@org_glassfish_jersey_core_jersey_client//jar",
    "@org_glassfish_jersey_core_jersey_common//jar",
    "@org_glassfish_jersey_media_jersey_media_json_jackson//jar",
    "@commons_cli_commons_cli//jar",
    "//heron/proto:proto_topology_java",
    "//heron/spi/src/java:utils-spi-java",
]

java_library(
    name = "healthmgr-java",
    srcs = glob(
        ["**/*.java"],
         ),
    deps = healthmgr_deps_files,
)

java_binary(
    name = "healthmgr-unshaded",
    srcs = glob(["**/*.java"]),
    deps = healthmgr_deps_files,
)

genrule(
    name = "heron-healthmgr",
    srcs = [":healthmgr-unshaded_deploy.jar"],
    outs = ["heron-healthmgr.jar"],
    cmd  = "cp $< $@",
)

