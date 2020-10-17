from output.file_name_converter import FileInfo, FileNameConverter


class TestFileNameConverter:
    def test_init(self):
        fnc = FileNameConverter()
        assert isinstance(fnc.data_product_paths, dict)

    # TODO: Improve test cases, edge cases?
    def test_convert(self):
        fnc = FileNameConverter()

        fname_1 = "/var/folders/6b/k8zzkvks79z_2ly7x7j94ph00000gn/T/tmp0bdbomk2/ela_l1_eng_20000101_v01.cdf"
        dest_1 = fnc.convert(fname_1)
        assert dest_1 == "/themis/data/elfin/ela/l1/eng/ela_l1_eng_20000101_v01.cdf"

        # State names must be handled differently
        fname_2 = "/usr/bin/elb_l1_state_defn_20200901_v01.cdf"
        dest_2 = fnc.convert(fname_2)
        assert dest_2 == "/themis/data/elfin/elb/l1/state/defn/elb_l1_state_defn_20200901_v01.cdf"


class TestFileInfo:
    def test_init(self):
        FileInfo("BLAH", 2, 1, "epdef")
