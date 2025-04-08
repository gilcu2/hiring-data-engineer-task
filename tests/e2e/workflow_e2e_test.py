from workflow import update_flow

def test_pipeline():
    assert update_flow(None, None) == 0
