import pytest 
from lib.Utils import get_spark_session
from lib.DataReader import read_customers,read_orders
from lib import ConfigReader
from lib.DataManipulation import filter_closed_orders,filter_orders_generic
from lib.ConfigReader import  get_app_config
from lib.DataManipulation import count_orders_state
import pytest

# @pytest.fixture
# def spark():
#     return get_spark_session("LOCAL")

@pytest.mark.skip("Work in Progress")
def test_read_app_config():
    # test_ will be automatically poiked as 1 unit test  by pytest
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"
@pytest.mark.skip("Work in Progress")
def test_read_customer_df(spark):
    # spark = get_spark_session("LOCAL") -> we get this from conftest.py automatically
    conf=ConfigReader.get_test_config("TEST")
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == int(conf["total_customers"])
@pytest.mark.skip("Work in Progress")
def test_read_orders_df(spark):
    # spark = get_spark_session("LOCAL")
    conf=ConfigReader.get_test_config("TEST")
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == int(conf["total_orders"])

#@pytest.mark.transformation -> Skipped for time being
@pytest.mark.skip("Work in Progress")
def test_closed_orders_df(spark):
    # spark = get_spark_session("LOCAL")
    conf=ConfigReader.get_test_config("TEST")
    closed_order_count = filter_closed_orders(read_orders(spark,"LOCAL")).count()
    assert closed_order_count == int(conf["filter_closed_orders"])
    
#@pytest.mark.transformation -> Skipped for time being
@pytest.mark.skip("Work in Progress")
def test_count_orders_state(spark,expected_results):
    # comparing 2 lists
    actual_results = count_orders_state(read_customers(spark,"LOCAL"))
    assert actual_results.collect() >= expected_results.collect()

@pytest.mark.parametrize(
        "status,count",
        [("CLOSED",7556),
         ("PENDING_PAYMENT",15030),
         ("COMPLETE",22900)
        ]
)
def test_status_count(spark,status,count):
    # spark = get_spark_session("LOCAL")
    filtered_count = filter_orders_generic(read_orders(spark,"LOCAL"),status).count()
    assert filtered_count == count

