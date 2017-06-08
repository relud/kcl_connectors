#!/usr/bin/env python
from amazon_kclpy.kcl import KCLProcess
from kcl_connectors import base
from os import getenv
import socket

class RecordProcessor(base.RecordProcessor):
    # Address where logs are sent over TCP
    address = (
        getenv('ADDRESS', '127.0.0.1:5565').split(':')[0],
        int(getenv('ADDRESS', '127.0.0.1:5565').split(':')[1]),
    )
    # socket for sending data
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def initialize(self, shard_id):
        base.RecordProcessor.initialize(self, shard_id)
        self.sock.connect(self.address)

    def process_record(self, data, partition_key, sequence_number, sub_sequence_number):
        self.sock.sendall(data)

    def shutdown(self, checkpointer, reason):
        self.sock.close()
        base.RecordProcessor.shutdown(self, checkpointer, reason)

if __name__ == "__main__":
    kclprocess = KCLProcess(RecordProcessor())
    kclprocess.run()
