#!/usr/bin/env python
from amazon_kclpy import kcl
from os import getenv
from sys import stderr
from time import sleep, time
from base64 import b64decode

class RecordProcessor(kcl.RecordProcessorBase):
    # checkpoint configs
    checkpoint_seconds = int(getenv('CHECKPOINT_SECONDS', 10))
    checkpoint_retries = int(getenv('CHECKPOINT_RETRIES', 5))
    checkpoint_retry_wait = int(getenv('CHECKPOINT_RETRY_WAIT', 5))
    largest_seq = None
    last_checkpoint_time = time()

    def initialize(self, shard_id):
        pass

    def checkpoint(self, checkpointer, sequence_number=None):
        for n in range(0, self.checkpoint_retries):
            try:
                checkpointer.checkpoint(sequence_number)
                return
            except kcl.CheckpointError as e:
                if e.value == 'ShutdownException':
                    print('Encountered shutdown execption, skipping checkpoint')
                    return
                elif e.value == 'ThrottlingException':
                    if self.checkpoint_retries - 1 == n:
                        stderr.write('Error while checkpointing: failed after %s attempts\n' % (n+1))
                        return
                    else:
                        print('Throttled while checkpointing: retry in %s seconds' % self.checkpoint_retry_wait)
                elif 'InvalidStateException' == e.value:
                    stderr.write('Error while checkpointing: invalid state\n')
                else: # Some other error
                    stderr.write('Error while checkpointing: %s\n' % e)
            sleep(self.checkpoint_retry_wait)

    def process_record(self, data, partition_key, sequence_number):
        pass

    def process_records(self, records, checkpointer):
        try:
            for record in records:
                data = b64decode(record['data'])
                key = record['partitionKey']
                seq = record['sequenceNumber']
                self.process_record(data, key, seq)
                seq = int(seq)
                if self.largest_seq is None or seq > self.largest_seq:
                    self.largest_seq = seq
            if time() - self.last_checkpoint_time > self.checkpoint_seconds:
                self.checkpoint(checkpointer, str(self.largest_seq))
                self.last_checkpoint_time = time()
        except Exception as e:
            stderr.write("Error processing records: %s\n" % e)

    def shutdown(self, checkpointer, reason):
        try:
            if reason == 'TERMINATE':
                print('Shutting down with checkpoint')
                self.checkpoint(checkpointer)
            else:
                print('Shutting down without checkpoint due to failover')
        except:
            pass

if __name__ == "__main__":
    kclprocess = kcl.KCLProcess(RecordProcessor())
    kclprocess.run()
