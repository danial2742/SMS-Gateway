def sms_cost(unit_cost: int, recipient_count: int = 1) -> int:
    """Money is integer minor units, never float (docs/database.md Design principles)."""
    return unit_cost * recipient_count
