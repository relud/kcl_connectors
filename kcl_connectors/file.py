#!/usr/bin/env python
from amazon_kclpy.kcl import KCLProcess
from datetime import datetime
from kcl_connectors import base
from os import getenv
from os import fstat
from os import stat

class RecordProcessor(base.RecordProcessor):
    # file where logs are written
    fname = getenv('FILE')
    fp = None
    fp_ino = None

    def process_record(self, data, partition_key, sequence_number, sub_sequence_number):
        if self.fp is None or self.fp_ino != stat(self.fname).st_ino:
            if self.fp is not None:
                self.fp.close()
            self.fp = open(self.fname, 'a')
            self.fp_ino = fstat(self.fp.fileno()).st_ino
        if not data or data[-1] != '\n':
            data += '\n'
        self.fp.write(data)
        self.fp.flush()

if __name__ == "__main__":
    kclprocess = KCLProcess(RecordProcessor())
    kclprocess.run()
