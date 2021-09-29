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
        assert dest_1 == "/nfs/elfin-data/ela/l1/eng/2000/ela_l1_eng_20000101_v01.cdf"

        # State names must be handled differently
        fname_2 = "/usr/bin/elb_l1_state_defn_20200901_v01.cdf"
        dest_2 = pfm.map_file(fname_2)
        assert dest_2 == "/nfs/elfin-data/elb/l1/state/defn/2020/elb_l1_state_defn_20200901_v01.cdf"

        fname_3 = "/usr/bin/elb_l0_fgm_20181011_354.pkt"
        dest_3 = pfm.map_file(fname_3)
        assert dest_3 == "/nfs/elfin-data/elb/l0/fgm/elb_l0_fgm_20181011_354.pkt"
