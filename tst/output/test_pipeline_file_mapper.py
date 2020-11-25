from output.pipeline_file_mapper import PipelineFileMapper
from util.constants import DATA_PRODUCT_PATHS, SERVER_BASE_DIR


class TestPipelineFileMapper:
    def test_init(self):
        pfm = PipelineFileMapper(SERVER_BASE_DIR, DATA_PRODUCT_PATHS)
        assert pfm.server_base_dir == SERVER_BASE_DIR
        assert pfm.data_product_paths == DATA_PRODUCT_PATHS

    # TODO: Improve test cases, edge cases?
    def test_convert(self):
        pfm = PipelineFileMapper(SERVER_BASE_DIR, DATA_PRODUCT_PATHS)

        fname_1 = "/var/folders/6b/k8zzkvks79z_2ly7x7j94ph00000gn/T/tmp0bdbomk2/ela_l1_eng_20000101_v01.cdf"
        dest_1 = pfm.map_file(fname_1)
        assert dest_1 == "/themis/data/elfin/ela/l1/eng/ela_l1_eng_20000101_v01.cdf"

        # State names must be handled differently
        fname_2 = "/usr/bin/elb_l1_state_defn_20200901_v01.cdf"
        dest_2 = pfm.map_file(fname_2)
        assert dest_2 == "/themis/data/elfin/elb/l1/state/defn/elb_l1_state_defn_20200901_v01.cdf"
