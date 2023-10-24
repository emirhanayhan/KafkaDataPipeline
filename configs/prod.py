from os import getenv

prod_config = {
    "kafka_host": getenv("KAFKA_HOST"),
    "topic_name": getenv("TOPIC_NAME"),
    "group_id": getenv("GROUP_ID"),
    "partitions_count": int(getenv("PARTITIONS_COUNT", 2)),
}
