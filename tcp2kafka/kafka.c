#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>
#include "tcp2kafka.h"

extern struct ini_list kafka_list[20];

void dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
	if (rkmessage->err) {
		ERR_PRINT("Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
	}
}

rd_kafka_t *new_rk()
{
	rd_kafka_t *rk;
	rd_kafka_conf_t *conf;
	rd_kafka_resp_err_t err;
	char errstr[512];

	int i = 0;

	conf = rd_kafka_conf_new();

	for (i = 0; i < 20; i++)
	{
		if (kafka_list[i].k[0] == 0x00)
			break;

		if (rd_kafka_conf_set(conf, kafka_list[i].k, kafka_list[i].v, errstr,
					sizeof(errstr)) != RD_KAFKA_CONF_OK) {
			ERR_PRINT("[%s]=[%s] %s\n", kafka_list[i].k, kafka_list[i].v, errstr);
			rd_kafka_conf_destroy(conf);
			return 0;
		}
	}

	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk) {
		ERR_PRINT("%% Failed to create new consumer: %s\n",
				errstr);
		return 0;
	}

	conf = NULL;

	return rk;
}

rd_kafka_topic_t *new_topic(rd_kafka_t *rk, char *topic_name)
{
	rd_kafka_topic_t *topic = rd_kafka_topic_new(rk, topic_name, NULL);
	if (!topic) {
		ERR_PRINT("Failed to create topic: %s\n", rd_kafka_err2str(rd_kafka_last_error()));
		return 0;
	}

	return topic;
}

void close_topic(rd_kafka_topic_t *topic_name)
{
	rd_kafka_topic_destroy(topic_name);
}

void close_rk(rd_kafka_t *rk)
{
	if(rk)
		rd_kafka_destroy(rk);
}

