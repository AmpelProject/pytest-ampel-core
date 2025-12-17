def test_mock_context_fixture(pytester):
    """Make sure that pytest accepts our fixture."""

    # create a temporary pytest test module
    pytester.makepyfile("""
        def test_a_thing(mock_context):
            t0_write = mock_context.db.get_collection("t0")
            t0_read = mock_context.db.get_collection("t0", mode="r")
            t0_write.insert_one({'key': 'value'})
            doc = t0_read.find_one({'key': 'value'})
            assert doc['key'] == 'value'
    """)

    # run pytest with the following cmd args
    result = pytester.runpytest("-v")

    result.assert_outcomes(passed=1)
