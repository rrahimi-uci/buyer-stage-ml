from logger import logger
import pandas as pd
import numpy as np
from sklearn.metrics import confusion_matrix


class KPIMetrics(object):
    @staticmethod
    def compute_precision_recall_on_test_data(real_csv_on_s3, 
                                              predicted_csv_on_s3, 
                                              labels=["Dreamer", "Casual Explorer", "Active Searcher", "Ready to Transact"]):
        try:
            df_real = pd.read_csv(real_csv_on_s3, header=None)
            real = df_real[df_real.columns[-1]].to_list()

            df_predicted = pd.read_csv(predicted_csv_on_s3, header=None)
            predicts = df_predicted[df_predicted.columns[-1]].to_list()

            cm = confusion_matrix(real, predicts, labels=labels)
            recall = np.diag(cm) / np.sum(cm, axis=1)
            precision = np.diag(cm) / np.sum(cm, axis=0)
            
            logger.info("The confusion matrix is :: " + str(cm))
            logger.info("The precision is :: " + str(precision))
            logger.info("The recall is :: " + str(recall))

            return

        except Exception as message:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            f_name = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error("filename = %s - function = compute_precision_recall in line_number = %s - hint = %s " % (
                f_name, exc_tb.tb_lineno, message.__str__()))
            return None