# Root-level conftest.py (not tests/conftest.py) so pytest's directory-based
# conftest discovery makes these fixtures visible to every service's
# tests/integration/ — those live under services/*/tests/, siblings of
# tests/, not descendants of it, so a plugin import is needed to reach them.
pytest_plugins = ["tests.conftest"]
