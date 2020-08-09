import logging
import smtplib
import time

from utils.credentials import EMAIL_USERNAME, EMAIL_PASSWORD
from util.science_utils import s_if_plural


class ExceptionCollector:
    def __init__(self, email_list):
        self.email_list = email_list
        self.exception_list = []

        self.logger = logging.getLogger("ExceptionCollector")

    def record_exception(self, pr, e, traceback_msg):
        self.logger.critical(f"Exception Recorded:\n\t\t{pr.to_string()}\n\t\t{e}\n\t\t{traceback_msg}")
        self.exception_list.append((pr, e, traceback_msg))

    def email_if_exceptions_occurred(self):
        if not self.exception_list:
            self.logger.info("No exceptions recorded")
            return

        with smtplib.SMTP('smtp.gmail.com', 587) as email_manager:
            email_manager.starttls()
            email_manager.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            email_manager.sendmail(EMAIL_USERNAME, self.email_list, self.generate_email())
            time.sleep(1)       # An attempt to avoid issues like the one that occured 2020-01-22: https://stackoverflow.com/questions/39097834/gmail-smtp-error-temporary-block

    def generate_email(self):
        subject = f"Subject: Exception{s_if_plural(self.exception_list)} occurred"
        seperator = "\n\n"
        body = "\n".join([f"{pr.to_string()}\n{e}\n{traceback_msg}" for pr, e, traceback_msg in self.exception_list])

        return subject + seperator + body
