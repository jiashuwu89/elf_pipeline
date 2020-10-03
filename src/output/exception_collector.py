"""Contains definition of class to record and report errors that occur"""
import logging
import smtplib
import time
from typing import Any, List

from util.science_utils import s_if_plural

try:
    from util.credentials import EMAIL_PASSWORD, EMAIL_USERNAME
except ModuleNotFoundError:
    EMAIL_PASSWORD, EMAIL_USERNAME = "", ""


class ExceptionCollector:
    """A class to record and report errors in log and via email.

    Parameters
    ----------
    email_list : List[str]
        A list of email addresses to which the ExceptionCollector should send
        any email notfications
    """

    def __init__(self, email_list: List[str]):
        self.email_list = email_list
        self.exception_list: List[str] = []

        self.logger = logging.getLogger(self.__class__.__name__)

    def record_exception(self, *to_record: Any) -> None:
        """Logs and stores all parameters passed to the method.

        If nothing is passed to record_exception, nothing will happen.
        """
        if len(to_record) == 0:
            return
        log_msg = "Exception Recorded:"
        email_msg = "Exception Recorded:"
        for x in to_record:
            log_msg += f"\n{x}"
            email_msg += f"\n{x}"
        self.logger.critical(log_msg)
        self.exception_list.append(email_msg)

    def email(self) -> None:
        """Sends an email containing all recorded errors

        To avoid issues like the one that occured 2020-01-22, a delay is
        inserted after sending the email:
        https://stackoverflow.com/questions/39097834/gmail-smtp-error-temporary-block
        """
        with smtplib.SMTP("smtp.gmail.com", 587) as email_manager:
            email_manager.starttls()
            email_manager.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            email_manager.sendmail(EMAIL_USERNAME, self.email_list, self.generate_email())

            time.sleep(1)

    def generate_email(self) -> str:
        """Creates a formatted email containing all recorded errors.

        Returns
        -------
        str
            A formatted email detailing the exceptions recorded.
        """
        subject = f"Subject: Exception{s_if_plural(self.exception_list)} occurred"
        seperator = "\n\n"
        body = "\n".join(self.exception_list)

        return subject + seperator + body

    @property
    def count(self) -> int:
        return len(self.exception_list)
