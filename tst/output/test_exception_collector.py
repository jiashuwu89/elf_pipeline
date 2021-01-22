from src.output.exception_collector import ExceptionCollector

ERROR = "error"
EMOJI_ERROR = '''f"⚠️ New Downlink, skipping current packet (ID: {science_packet.science_packet_id})"'''


class TestExceptionCollector:
    def test_record_exception(self):
        # GIVEN
        exception_collector = ExceptionCollector([])

        # WHEN
        exception_collector.record_exception()
        exception_collector.record_exception(ERROR)
        exception_collector.record_exception(ERROR, ERROR)
        exception_collector.record_exception(ERROR, ERROR, ERROR)

        # THEN
        assert len(exception_collector.exception_list) == 3

    def test_generate_email(self):
        # GIVEN
        exception_collector = ExceptionCollector([])

        # WHEN
        exception_collector.record_exception(ERROR)
        exception_collector.record_exception(EMOJI_ERROR)
        email_msg = exception_collector.generate_email()

        # THEN
        assert email_msg != ""
        assert email_msg.encode("ascii") != ""  # Make sure message can be encoded to be sent

    def test_count(self):
        exception_collector = ExceptionCollector([])
        assert exception_collector.count == 0

        exception_collector.record_exception(ERROR)
        assert exception_collector.count == 1
