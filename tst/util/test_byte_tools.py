from src.util import byte_tools


class TestByteTools:
    def test_little_e_to_big_e(self):
        assert byte_tools.little_e_to_big_e(b"badcfe") == b"abcdef"

    def test_raw_idpu_bytes_to_datetime(self):
        pass

    def test_get_signed(self):
        assert byte_tools.get_signed(b"1234") == 825373492

    def test_get_three_signed_bytes(self):
        pass

    def test_get_two_unsigned_bytes(self):
        assert byte_tools.get_two_unsigned_bytes(1) == b"\x00\x01"
        assert byte_tools.get_two_unsigned_bytes(15) == b"\x00\x0f"
        assert byte_tools.get_two_unsigned_bytes(65535) == b"\xff\xff"
        assert byte_tools.get_two_unsigned_bytes(100000) == b"\xff\xff"  # BUG?

    def test_bin_string(self):
        pass

    def test_get_huffman(self):
        pass
