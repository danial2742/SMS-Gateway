from gateway_common.domain.cost import sms_cost


def test_single_sms_cost_is_unit_cost():
    assert sms_cost(1) == 1
    assert sms_cost(5) == 5


def test_batch_cost_scales_with_recipient_count():
    assert sms_cost(1, recipient_count=25000) == 25000
    assert sms_cost(2, recipient_count=3) == 6
