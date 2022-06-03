import apache_beam as beam
import logging
from google.cloud import storage


class DeleteFileGcs(beam.DoFn):

    def process(self, element, bucket_name):

        storage_client = storage.Client()

        blobs = storage_client.list_blobs(bucket_name, prefix='2022')

        for blob in blobs:
            path_name = blob.name
            # path = "/".join(path_name.split('/')[1:])
            searched_blobs = storage_client.list_blobs(bucket_name, prefix=path)
            for found_blob in searched_blobs:
                # found_blob.delete()
                logging.info("Found - {}".format(path_name))
