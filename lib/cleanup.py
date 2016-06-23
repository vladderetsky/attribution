import os
import shutil
import traceback


class CleanupException(Exception):
    pass


def cleanup_local_fs(path):
    """
    It cleans a file path. Spark cannot write the file with the same
    name (rewrite). Thus, previously existed path will be removed with its
    content before the next '.saveAsTextFile()' action.
    :param path: Directory name
    """
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except shutil.Error as e:
            raise CleanupException("Path removing error: {0}".format(
                ' '.join(traceback.format_exception_only(type(e), e))))


def cleanup_hdfs():
    """
    It can be extended when implementation requires the Spark cluster usage
    and data will be saved to HDFS file system
    :return:
    """
    pass


def cleanup_s3():
    """
    It can be extended when implementation requires the Spark cluster usage
    and data will be saved to S3 file system
    :return:
    """
    pass
