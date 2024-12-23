import pytest 
from lib.Utils import get_spark_session

# Fixing 
#decorater
@pytest.fixture
def spark():
    # setup
    spark_session = get_spark_session("LOCAL")
    # before unit test 
    yield spark_session
    # after unit test any code [after yeild will run after the test cases are completed - teardown]
    spark_session.stop()



@pytest.fixture
def expected_results(spark):
    # returns expected results 
    result_schema = 'state string, count int'
    return spark.read \
            .format("csv") \
            .schema(result_schema) \
            .option("header","true") \
            .load("data/test_result/state_aggregate.csv")