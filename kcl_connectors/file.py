#!/usr/bin/env python
from amazon_kclpy.kcl import KCLProcess
from datetime import datetime
from kcl_connectors import base
from os import getenv

class RecordProcessor(base.RecordProcessor):
    # file where logs are written
    fp = open(getenv('FILE'), 'a')
    # when self.fp was opened
    fp_opened = datetime.now()
    # seconds we write to self.fp before reopening it
    fp_max_age = int(getenv('FILE_MAX_AGE', 60))

    def process_record(self, data, partition_key, sequence_number):
        if (datetime.now() - self.fp_opened).seconds > self.fp_max_age:
            self.fp.close()
            self.fp = open(getenv('FILE'), 'a')
            self.fp_opened = datetime.now()
        self.fp.write(data)
        self.fp.flush()

if __name__ == "__main__":
    kclprocess = KCLProcess(RecordProcessor())
    kclprocess.run()
