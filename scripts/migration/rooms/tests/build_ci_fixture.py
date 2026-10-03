"""Create signed synthetic source data only, never read production data."""
from pathlib import Path
import shutil
import sys

sys.path.insert(0, str(Path(__file__).parent))
from test_plan import NativePlanTests

fixture = NativePlanTests()
fixture.setUp()
try:
    fixture.source.execute("INSERT INTO users(user_id,password_hash) VALUES (?,NULL)", (fixture.user,))
    fixture.source.commit()
    shutil.copyfile(fixture.path, sys.argv[1])
finally:
    fixture.tearDown()
