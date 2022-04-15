import os
import tempfile

import pytest

from processor.science_processor import ScienceProcessor
from util.constants import ALL_MISSIONS, IDPU_PRODUCTS, MISSION_DICT, MRM_PRODUCTS
from util.dummy import DummyPipelineConfig, DummyProcessingRequest, DummyScienceProcessor


class TestScienceProcessor:
    def test_init__(self):
        processing_request = DummyProcessingRequest()

        with pytest.raises(TypeError):
            ScienceProcessor(processing_request)

    def test_generate_files(self):
        pipeline_config = DummyPipelineConfig()
        processing_request = DummyProcessingRequest()

        science_processor = DummyScienceProcessor(pipeline_config)
        with pytest.raises(NotImplementedError):
            science_processor.generate_files(processing_request)

    def test_make_filename(self):
        pipeline_config = DummyPipelineConfig()
        processing_request = DummyProcessingRequest()
        science_processor = DummyScienceProcessor(pipeline_config)

        assert (
            science_processor.make_filename(processing_request, 0, 100)
            == f"{pipeline_config.output_dir}/ela_l0_epdef_20200805_100.pkt"
        )
        assert (
            science_processor.make_filename(processing_request, 1)
            == f"{pipeline_config.output_dir}/ela_l1_epdef_20200805_v01.cdf"
        )

        # Level 0 with no size should fail
        with pytest.raises(ValueError):
            science_processor.make_filename(processing_request, 0)
        with pytest.raises(ValueError):
            science_processor.make_filename(processing_request, 2)

    def test_create_empty_cdf(self):
        pipeline_config = DummyPipelineConfig()
        science_processor = DummyScienceProcessor(pipeline_config)

        ALL_LEVELS = [1]

        base_names = [
            f"{MISSION_DICT[mission_id]}_l{level}_{product}_20220405_v01.cdf"
            for mission_id in ALL_MISSIONS
            for level in ALL_LEVELS
            for product in IDPU_PRODUCTS + MRM_PRODUCTS + ["state_defn", "state_pred"]
        ]

        for fname in base_names:
            with tempfile.TemporaryDirectory() as temp_dir_name:
                full_name = os.path.join(temp_dir_name, fname)
                cdf = science_processor.create_empty_cdf(full_name)

                assert cdf is not None
                assert os.path.isfile(full_name)
