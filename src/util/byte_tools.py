"""Utilities to operate on data in the form of bytes"""
import elfin.libelfin.utils as _utils


def little_e_to_big_e(data):
    temp = b""
    while data:
        temp += data[1:2] + data[0:1]
        data = data[2:]
    return temp


def raw_idpu_bytes_to_datetime(x):
    return _utils.timestamp_to_datetime(little_e_to_big_e(x[0:8]), "idpu")


def get_signed(b):
    return int.from_bytes(b, "big", signed=True)


def get_three_signed_bytes(i):
    return i.to_bytes(3, "big", signed=True)


def get_two_unsigned_bytes(i):
    try:
        return i.to_bytes(2, "big")
    except OverflowError:
        return b"\xff\xff"


def bin_string(b):
    s = ""
    for byte in b:
        s += "{:08b}".format(byte)
    return s


def get_huffman(bitstring, table):
    b = bitstring[0:1]
    bitstring = bitstring[1:]
    while b not in table:
        if len(b) > 12 or len(bitstring) == 0:
            raise IndexError(f"Unable to find huffman string: len(b): {len(b)}, len(bitstring): {len(bitstring)}")

        b += bitstring[0:1]
        bitstring = bitstring[1:]
        # TODO: May want to make this more dynamic depending on the table...

    val = table[b]
    return val, bitstring
