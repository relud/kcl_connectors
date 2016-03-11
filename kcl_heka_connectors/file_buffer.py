#!/usr/bin/env python
from amazon_kclpy import kcl
import json, base64, os

class RecordProcessor(kcl.RecordProcessorBase):
    # file where logs are written
    file_buffer = open(os.getenv('FILE_BUFFER'), 'a')

    def initialize(self, shard_id):
        pass

    def process_records(self, records, checkpointer):
        for record in records:
            self.file_buffer.write(base64.b64decode(record['data']))
            self.file_buffer.flush()

    def shutdown(self, checkpointer, reason):
        pass

if __name__ == "__main__":
    kclprocess = kcl.KCLProcess(RecordProcessor())
    kclprocess.run()
