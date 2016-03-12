#!/usr/bin/env python
from amazon_kclpy.kcl import KCLProcess
from kcl_heka_connectors import base
from os import getenv

class RecordProcessor(base.RecordProcessor):
    # file where logs are written
    file_buffer = open(getenv('FILE_BUFFER'), 'a')

    def process_record(self, data, partition_key, sequence_number):
        self.file_buffer.write(data)
        self.file_buffer.flush()

if __name__ == "__main__":
    kclprocess = KCLProcess(RecordProcessor())
    kclprocess.run()
