"""Compression Values, used to decompress compressed data
- Items related to FGM Compression
- Items related to EPD Compression
"""

# FGM Compression Information
FGM_HUFFMAN = {
    "0110": 0,
    "0111": 1,
    "101": 2,
    "10011": 3,
    "1111": 4,
    "1000": 5,
    "00": 6,
    "110": 7,
    "010": 8,
    "1110": 9,
    "100101": 10,
    "100100100": 11,
    "100100101": 12,
    "100100110": 13,
    "100100111": 14,
    "1001000": 15,
}


# EPD Compression information
EPD_HUFFMAN = {
    "1": 0,
    "01": 15,
    "001": 1,
    "00000": 10,
    "00001000": 4,
    "00001001": 7,
    "000010100": 3,
    "00001010100": 6,
    "00001010101": 11,
    "000010101100": 2,
    "000010101101": 5,
    "00001010111": 12,
    "00001011": 8,
    "000011": 14,
    "00010": 13,
    "00011": 9,
}

EPD_LOSSY_VALS = [
    [
        0,
        100,
        202,
        305,
        408,
        513,
        618,
        724,
        832,
        940,
        1049,
        1160,
        1271,
        1383,
        1497,
        1611,
        1727,
        1843,
        1960,
        2079,
        2199,
        2319,
        2441,
        2564,
        2688,
        2813,
        2940,
        3067,
        3196,
        3326,
        3457,
        3589,
        3722,
        3857,
        3992,
        4129,
        4268,
        4407,
        4548,
        4690,
        4833,
        4978,
        5124,
        5271,
        5420,
        5570,
        5721,
        5874,
        6028,
        6183,
        6340,
        6498,
        6658,
        6819,
        6982,
        7146,
        7312,
        7479,
        7647,
        7817,
        7989,
        8162,
        8337,
        8514,
        8692,
        8871,
        9053,
        9235,
        9420,
        9606,
        9794,
        9984,
        10175,
        10368,
        10563,
        10760,
        10958,
        11158,
        11360,
        11564,
        11770,
        11977,
        12187,
        12398,
        12611,
        12827,
        13044,
        13263,
        13484,
        13707,
        13932,
        14159,
        14388,
        14620,
        14853,
        15089,
        15326,
        15566,
        15808,
        16052,
        16299,
        16547,
        16798,
        17051,
        17307,
        17565,
        17825,
        18087,
        18352,
        18619,
        18889,
        19161,
        19436,
        19713,
        19992,
        20275,
        20559,
        20846,
        21136,
        21429,
        21724,
        22022,
        22322,
        22626,
        22932,
        23241,
        23552,
        23867,
        24184,
        24504,
        24827,
        25153,
        25482,
        25814,
        26149,
        26487,
        26828,
        27172,
        27519,
        27870,
        28223,
        28580,
        28940,
        29303,
        29670,
        30040,
        30413,
        30790,
        31170,
        31553,
        31940,
        32331,
        32725,
        33123,
        33524,
        33929,
        34337,
        34749,
        35165,
        35585,
        36009,
        36436,
        36867,
        37303,
        37742,
        38185,
        38632,
        39083,
        39539,
        39998,
        40462,
        40929,
        41401,
        41878,
        42358,
        42843,
        43333,
        43827,
        44325,
        44828,
        45335,
        45847,
        46364,
        46885,
        47411,
        47942,
        48478,
        49018,
        49564,
        50114,
        50669,
        51230,
        51795,
        52366,
        52942,
        53523,
        54109,
        54701,
        55298,
        55900,
        56508,
        57121,
        57740,
        58365,
        58995,
        59631,
        60272,
        60920,
        61573,
        62233,
        62898,
        63569,
        64247,
        64930,
        65620,
        66316,
        67018,
        67727,
        68442,
        69164,
        69892,
        70626,
        71368,
        72116,
        72871,
        73633,
        74401,
        75177,
        75960,
        76750,
        77546,
        78351,
        79162,
        79981,
        80807,
        81641,
        82482,
        83331,
        84188,
        85052,
        85925,
        86805,
        87693,
        88589,
        89494,
        90406,
        91327,
        92256,
        93194,
        94140,
        95095,
        96058,
        97030,
        98011,
        99001,
        100000,
    ],
    [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        45,
        46,
        47,
        49,
        51,
        53,
        55,
        57,
        59,
        61,
        63,
        65,
        67,
        69,
        71,
        73,
        75,
        77,
        80,
        83,
        86,
        89,
        92,
        95,
        98,
        101,
        104,
        107,
        110,
        114,
        118,
        122,
        126,
        130,
        134,
        138,
        142,
        147,
        152,
        157,
        162,
        167,
        172,
        178,
        184,
        190,
        196,
        202,
        209,
        216,
        223,
        230,
        237,
        245,
        253,
        261,
        270,
        279,
        288,
        297,
        307,
        317,
        327,
        338,
        349,
        360,
        372,
        384,
        397,
        410,
        423,
        437,
        451,
        466,
        481,
        497,
        513,
        530,
        547,
        565,
        583,
        602,
        622,
        642,
        663,
        685,
        707,
        730,
        754,
        779,
        804,
        830,
        857,
        885,
        914,
        944,
        975,
        1007,
        1040,
        1074,
        1109,
        1145,
        1182,
        1221,
        1261,
        1302,
        1344,
        1388,
        1433,
        1480,
        1528,
        1578,
        1629,
        1682,
        1737,
        1794,
        1852,
        1912,
        1974,
        2038,
        2104,
        2173,
        2244,
        2317,
        2393,
        2471,
        2552,
        2635,
        2721,
        2810,
        2902,
        2997,
        3095,
        3196,
        3300,
        3408,
        3519,
        3634,
        3752,
        3874,
        4000,
        4130,
        4265,
        4404,
        4548,
        4696,
        4849,
        5007,
        5170,
        5339,
        5513,
        5693,
        5879,
        6071,
        6269,
        6473,
        6684,
        6902,
        7127,
        7359,
        7599,
        7847,
        8103,
        8367,
        8640,
        8922,
        9213,
        9513,
        9823,
        10143,
        10474,
        10815,
        11168,
        11532,
        11908,
        12296,
        12697,
        13111,
        13538,
        13979,
        14435,
        14906,
        15392,
        15894,
        16412,
        16947,
        17499,
        18069,
        18658,
        19266,
        19894,
        20543,
        21213,
        21905,
        22619,
        23356,
        24117,
        24903,
        25715,
        26553,
        27419,
        28313,
        29236,
        30189,
        31173,
        32189,
        33238,
        34322,
        35441,
        36596,
        37789,
    ],
]
