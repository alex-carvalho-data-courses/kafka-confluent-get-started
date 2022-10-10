#!/usr/bin/env python

from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING


if __name__ == '__main__':
    # Parse the command line
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    # Create consumer instance
    _consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    topic = 'purchases'
    _consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from kafka and print them
    try:
        while True:
            msg = _consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to `session.timeout.ms`
                # fot the consumer group to rebalance and start consuming
                print('Waiting')
            elif msg.error():
                print(f'ERROR: {msg.error()}')
            else:
                # Extract the (optional) key and value, and print
                print(f'Consumed event from topic {topic}: '
                      f'key = {msg.key().decode("utf-8"):12} '
                      f'value = {msg.value().decode("utf-8")}')

    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        _consumer.close()
