from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

def run_notebook(path, params={}):
    dbutils = DBUtils(SparkSession.builder.getOrCreate())
    dbutils.notebook.run(path=path, timeout_seconds=0, arguments=params)

def get_param(param):
    try:
        dbutils = DBUtils(SparkSession.builder.getOrCreate())
        return dbutils.widgets.get(param)
    except Exception as e:
        return None