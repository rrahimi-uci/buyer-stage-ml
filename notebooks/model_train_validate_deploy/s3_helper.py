from logger import logger
import boto3
import pickle
import s3fs

class S3Operations(object):
    
    @staticmethod
    def create_bucket(bucket):
        try:
            s3 = boto3.resource('s3')
            s3.create_bucket(Bucket=bucket)
            return True
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = create_bucket in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return False
    
    @staticmethod
    def s3_clean_up(bucket, prefix):
        try:
            s3 = boto3.resource('s3')
            bk = s3.Bucket(bucket)
            bk.objects.filter(Prefix=prefix).delete()
            return True
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = s3_clean_up in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return False

    @staticmethod
    def load_data_from_s3(s3_file_path, dest_name):
        try:
            fs = s3fs.S3FileSystem()
            with fs.open(s3_file_path, "rb") as f1:
                with open(dest_name, "wb") as f2:
                    f2.write(f1.read())
            return True
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = load_data_from_s3 in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None

    @staticmethod
    def pickle_file_s3(p, pkl_path):
        try:
            fs = s3fs.S3FileSystem()
            with fs.open(pkl_path, 'wb') as f:
                pickle.dump(p, f, protocol=2)
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = pickle_file_s3 in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None

    @staticmethod
    def unpickle_file_s3(pkl_path):
        try:
            fs = s3fs.S3FileSystem()
            with fs.open(pkl_path, 'rb') as f:
                p = pickle.load(f)
            return p
        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = unpickle_file_s3 in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None
