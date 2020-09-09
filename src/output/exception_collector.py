import logging
import smtplib
import time

from util.science_utils import s_if_plural
from utils.credentials import EMAIL_PASSWORD, EMAIL_USERNAME


class ExceptionCollector:
    def __init__(self, email_list):
        self.email_list = email_list
        self.exception_list = []

        self.logger = logging.getLogger(self.__class__.__name__)

    def record_exception(self, *to_record):
        log_msg = "Exception Recorded:"
        email_msg = "Exception Recorded:"
        for x in to_record:
            log_msg += f"\n\t\t{x}"
            log_msg += f"\n{x}"
        self.logger.critical(log_msg)
        self.exception_list.append(email_msg)

    def email(self):
        with smtplib.SMTP("smtp.gmail.com", 587) as email_manager:
            email_manager.starttls()
            email_manager.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            email_manager.sendmail(EMAIL_USERNAME, self.email_list, self.generate_email())
            # An attempt to avoid issues like the one that occured 2020-01-22: https://stackoverflow.com/questions/39097834/gmail-smtp-error-temporary-block
            time.sleep(1)

    def generate_email(self):
        subject = f"Subject: Exception{s_if_plural(self.exception_list)} occurred"
        seperator = "\n\n"
        body = "\n".join(self.exception_list)

        return subject + seperator + body
