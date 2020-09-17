import pytest

from util import byte_tools
from util.compression_values import FGM_HUFFMAN


class TestByteTools:
    def test_little_e_to_big_e(self):
        assert byte_tools.little_e_to_big_e(b"badcfe") == b"abcdef"

    # def test_raw_idpu_bytes_to_datetime(self):
    #     pass

    def test_get_signed(self):
        assert byte_tools.get_signed(b"1234") == 825373492

    def test_get_three_signed_bytes(self):
        pass

    def test_bin_string(self):
        assert byte_tools.bin_string(b"123") == "001100010011001000110011"
        assert byte_tools.bin_string(b"\x00") == "00000000"
        assert byte_tools.bin_string(b"james") == "0110101001100001011011010110010101110011"

    def test_get_two_unsigned_bytes(self):
        assert byte_tools.get_two_unsigned_bytes(1) == b"\x00\x01"
        assert byte_tools.get_two_unsigned_bytes(15) == b"\x00\x0f"
        assert byte_tools.get_two_unsigned_bytes(65535) == b"\xff\xff"
        assert byte_tools.get_two_unsigned_bytes(100000) == b"\xff\xff"

    # def test_bin_string(self):
    #     pass

    def test_get_huffman(self):
        assert byte_tools.get_huffman("01100111", FGM_HUFFMAN) == (0, "0111")
        for key, value in FGM_HUFFMAN.items():
            assert byte_tools.get_huffman(key, FGM_HUFFMAN) == (value, "")

        with pytest.raises(IndexError):
            byte_tools.get_huffman("", FGM_HUFFMAN)
