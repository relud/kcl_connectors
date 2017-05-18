#!/usr/bin/env python
from amazon_kclpy.kcl import KCLProcess
from datetime import datetime
from kcl_connectors import base
from os import getenv

class RecordProcessor(base.RecordProcessor):
    # file where logs are written
    file_buffer = open(getenv('FILE_BUFFER'), 'a')
    file_buffer_opened = datetime.now()
    file_buffer_max_age = int(getenv('FILE_BUFFER_MAX_AGE', 60))

    def process_record(self, data, partition_key, sequence_number):
        if (datetime.now() - self.file_buffer_opened).seconds > self.file_buffer_max_age:
            self.file_buffer.close()
            self.file_buffer = open(getenv('FILE_BUFFER'), 'a')
            self.file_buffer_opened = datetime.now()
        self.file_buffer.write(data)
        self.file_buffer.flush()

if __name__ == "__main__":
    kclprocess = KCLProcess(RecordProcessor())
    kclprocess.run()
