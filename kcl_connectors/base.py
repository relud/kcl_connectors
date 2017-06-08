#!/usr/bin/env python
from amazon_kclpy import kcl
from amazon_kclpy.v2 import processor
from os import getenv
from sys import stderr
from time import sleep, time
from base64 import b64decode

class RecordProcessor(processor.RecordProcessorBase):
    # checkpoint configs
    checkpoint_seconds = int(getenv('CHECKPOINT_SECONDS', 10))
    checkpoint_retries = int(getenv('CHECKPOINT_RETRIES', 5))
    checkpoint_retry_wait = int(getenv('CHECKPOINT_RETRY_WAIT', 5))
    largest_seq = (None, None)
    largest_sub_seq = None
    last_checkpoint_time = None

    def initialize(self, shard_id):
        self.largest_seq = (None, None)
        self.last_checkpoint_time = time()

    def checkpoint(self, checkpointer, sequence_number=None, sub_sequence_number=None):
        for n in range(0, self.checkpoint_retries):
            try:
                checkpointer.checkpoint(sequence_number, sub_sequence_number)
                return
            except kcl.CheckpointError as e:
                if e.value == 'ShutdownException':
                    print('Encountered shutdown execption, skipping checkpoint')
                    return
                elif e.value == 'ThrottlingException':
                    if self.checkpoint_retries - 1 == n:
                        stderr.write('Failed to checkpoint after %s attempts, giving up.\n' % (n+1))
                        return
                    else:
                        print('Was throttled while checkpointing, will attempt again in %s seconds' % self.checkpoint_retry_wait)
                elif 'InvalidStateException' == e.value:
                    stderr.write('MultiLangDaemon reported an invalid state while checkpointing.\n')
                else: # Some other error
                    stderr.write('ncountered an error while checkpointing, error was %s.\n' % e)
            sleep(self.checkpoint_retry_wait)

    def should_update_sequence(self, sequence_number, sub_sequence_number):
        return self.largest_seq == (None, None) or sequence_number > self.largest_seq[0] or \
            (sequence_number == self.largest_seq[0] and sub_sequence_number > self.largest_seq[1])

    def process_record(self, data, partition_key, sequence_number, sub_sequence_number):
        pass

    def process_records(self, process_records_input):
        try:
            for record in process_records_input.records:
                data = record.binary_data
                key = record.partition_key
                seq = int(record.sequence_number)
                sub_seq = record.sub_sequence_number
                self.process_record(data, key, seq, sub_seq)
                if self.should_update_sequence(seq, sub_seq):
                    self.largest_seq = (seq, sub_seq)
            if time() - self.last_checkpoint_time > self.checkpoint_seconds:
                self.checkpoint(process_records_input.checkpointer, str(self.largest_seq[0]), self.largest_seq[1])
                self.last_checkpoint_time = time()
        except Exception as e:
            # catch the exception, checkpoint, and refuse to continue
            stderr.write("Encountered an exception while processing records. Exception was %s\n" % e)
            self.checkpoint(process_records_input.checkpointer, str(self.largest_seq[0]), self.largest_seq[1])
            self.last_checkpoint_time = time()
            exit(1)

    def shutdown(self, shutdown_input):
        try:
            if shutdown_input.reason == 'TERMINATE':
                print('Was told to terminate, will attempt to checkpoint.')
                self.checkpoint(shutdown_input.checkpointer, None)
            else: # reason == 'ZOMBIE'
                print('Shutting down due to failover. Will not checkpoint.')
        except:
            pass

if __name__ == "__main__":
    kclprocess = kcl.KCLProcess(RecordProcessor())
    kclprocess.run()
