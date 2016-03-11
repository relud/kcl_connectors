#!/usr/bin/env python
from amazon_kclpy import kcl
from os import getenv
import base64, socket

class RecordProcessor(kcl.RecordProcessorBase):
    # Address where logs are sent over TCP
    address = (
        getenv('ADDRESS', '127.0.0.1:5565').split(':')[0],
        int(getenv('ADDRESS', '127.0.0.1:5565').split(':')[1]),
    )
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def initialize(self, shard_id):
        self.sock.connect(self.address)

    def process_records(self, records, checkpointer):
        for record in records:
            self.sock.sendall(base64.b64decode(record['data']))

    def shutdown(self, checkpointer, reason):
        self.sock.close()

if __name__ == "__main__":
    kclprocess = kcl.KCLProcess(RecordProcessor())
    kclprocess.run()
