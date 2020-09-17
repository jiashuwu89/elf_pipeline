from src.output.exception_collector import ExceptionCollector

ERROR = "error"


# class TestExceptionCollector:
#     def test_record_exception(self):
#         # GIVEN
#         exception_collector = ExceptionCollector([])

#         # WHEN
#         exception_collector.record_exception()
#         exception_collector.record_exception(ERROR)
#         exception_collector.record_exception(ERROR, ERROR)
#         exception_collector.record_exception(ERROR, ERROR, ERROR)

#         # THEN
#         assert len(exception_collector.exception_list) == 3

#     def test_generate_email(self):
#         # GIVEN
#         exception_collector = ExceptionCollector([])

#         # WHEN
#         exception_collector.record_exception(ERROR)
#         email_msg = exception_collector.generate_email()

#         # THEN
#         assert email_msg != ""
