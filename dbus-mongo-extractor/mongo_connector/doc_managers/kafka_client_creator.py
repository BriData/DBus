# Copyright 2013-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from kafka import KafkaProducer
from kafka import KafkaConsumer

LOG = logging.getLogger(__name__)


class KafkaClientCreator(object):
    def __init__(self, ds_name):
        self._ds_name = ds_name

    def create_producer(self, producer_props):
        # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
        config = self._producer_props_to_config(producer_props)
        producer = KafkaProducer(**config)
        LOG.info("init producer to kafka.")
        return producer

    def _producer_props_to_config(self, producer_props):
        if producer_props is None:
            default_producer_config = {
                'bootstrap_servers': 'localhost:9092',
                'acks': 'all',
                'retries': 3,
                'compression_type': 'none',
                'batch_size': 1048576,
                'linger_ms': 1000,
                'max_request_size': 10485760
            }

            LOG.info("use default producer config. ")
            return default_producer_config
        else:
            producer_props["retries"] = int(producer_props["retries"])
            producer_props["batch_size"] = int(producer_props["batch_size"])
            producer_props["linger_ms"] = int(producer_props["linger_ms"])
            producer_props["max_request_size"] = int(producer_props["max_request_size"])
            producer_props["buffer_memory"] = int(producer_props["buffer_memory"])
            return producer_props

    def create_consumer(self, topics, consumer_props):
        # https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
        config = self._consumer_props_to_config(consumer_props)
        consumer = KafkaConsumer(*topics, **config)
        LOG.info("init consumer to kafka.")
        return consumer

    def _consumer_props_to_config(self, consumer_props):
        if consumer_props is None:
            default_consumer_config = {
                'bootstrap_servers': 'localhost:9092',
                'enable_auto_commit': 'true',
                'auto_commit_interval_ms': 1000,
                'session_timeout_ms': 30000,
                'max_partition_fetch_bytes': 10485760,
                'max_poll_records': 30,
                'client_id': 'mtestdb-mongo-extractor-consumer',
                'group_id': 'mtestdb-mongo-extractor-consumer'
            }
            return default_consumer_config
        else:
            consumer_props["auto_commit_interval_ms"] = int(consumer_props["auto_commit_interval_ms"])
            consumer_props["session_timeout_ms"] = int(consumer_props["session_timeout_ms"])
            consumer_props["max_partition_fetch_bytes"] = int(consumer_props["max_partition_fetch_bytes"])
            consumer_props["max_poll_records"] = int(consumer_props["max_poll_records"])
            consumer_props["client_id"] = self._ds_name + '-mongo-extractor-consumer'
            consumer_props["group_id"] = self._ds_name + '-mongo-extractor-consumer'

            return consumer_props
