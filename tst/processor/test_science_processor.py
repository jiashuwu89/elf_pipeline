import pytest

from dummy import DummyPipelineConfig, DummyProcessingRequest, DummyScienceProcessor
from processor.science_processor import ScienceProcessor


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
