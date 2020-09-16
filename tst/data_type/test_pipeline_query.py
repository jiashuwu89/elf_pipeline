from data_type.pipeline_query import PipelineQuery


class TestPipelineQuery:
    def test_data_products_to_idpu_types(self):
        assert PipelineQuery.data_products_to_idpu_types([]) == []
        assert PipelineQuery.data_products_to_idpu_types(["eng"]) == [14, 15, 16]
        assert sorted(PipelineQuery.data_products_to_idpu_types(["epdef", "fgs"])) == [1, 2, 3, 4]
