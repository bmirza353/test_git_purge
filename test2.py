import apache_beam as beam
import logging
from google.cloud import storage


class CopyDeleteFileGcs(beam.DoFn):

    def process(self, element, bucket_name):

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        def copy_file(dt_prefix, path_name):
            try:
                blob = bucket.blob(path_name)
                bucket.copy_blob(blob, bucket, dt_prefix + '/' + path_name)
                archive_blob = bucket.blob(dt_prefix + '/' + path_name)

                if archive_blob.exists():
                    return "SUCCESS"
                else:
                    return "FAILED"
            except Exception as error:
                return "FAILED"

        blobs = storage_client.list_blobs(bucket_name, prefix='inbound')
        for blob in blobs:
            path_name = blob.name
            if path_name.startswith('inbound'):
                dt_prefix = blob.time_created.strftime('%Y-%m-%d')
                res = copy_file(dt_prefix, path_name)
                if res == 'SUCCESS':
                    logging.info("Path - {} is successfully copied".format(path_name))
                    blob.delete()
                    logging.info("Path - {} is deleted".format(path_name))
                else:
                    logging.info("Path - {} is failed to copy".format(path_name))



