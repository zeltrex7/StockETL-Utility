from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row,SparkSession
import random as rand
spark = SparkSession.builder.master("local").appName("auto_etl").getOrCreate()
"""
CREATE TABLE `table_mater` (
  `table_mstr_key` int primary key,
  `table_name` varchar(100),
  `is_temp` boolean,
  `is_active` boolean
);

"""
N_SAMPLE = 5
table_mstr_key = [i for i in range(0,N_SAMPLE)]
table_name = ['table_name_' + str(i) for i in range(0,N_SAMPLE)]
is_temp = [False]*N_SAMPLE
is_active = [True]*N_SAMPLE


#print(table_mstr_key,table_name,is_temp,is_active)
table_master_df = pd.DataFrame({
    'table_mstr_key': table_mstr_key,
    'table_name': table_name,
    'is_temp': is_temp,
    'is_active': is_active,
})
table_master_df = spark.createDataFrame(table_master_df)
table_master_df.show()

"""
CREATE TABLE `data_type_master` (
  `data_type_mstr_key` int primary key,
  `data_type` varchar(100),
  `is_active` boolean,
  `is_current` boolean
);
"""

data_type_master_df = pd.DataFrame({
    'data_type_mstr_key': [0,1,2,3,4],
    'data_type': ['int','decimal','datetime','boolean','text'],
    'is_current': [True]*5,
    'is_active': [True]*5,
})
data_type_master_df = spark.createDataFrame(data_type_master_df)

data_type_master_df.show()
"""
CREATE TABLE `table_field_mater` (
  `table_field_mstr_key` int primary key,
  `field_name` varchar(100),
  `data_type_mstr_key` varchar(100),
  `is_active` boolean,
  `is_current` boolean,
  `table_mstr_key` int,
  FOREIGN KEY (`table_mstr_key`) REFERENCES `table_mater`(`table_mstr_key`)
);

"""
N_COL_SAMPLE=5
table_field_mstr_key = [i for i in range(0,N_SAMPLE*N_COL_SAMPLE)]
#print(table_field_mstr_key)
field_name = ['field_name_'+str(i) for i in range(0,N_SAMPLE*N_COL_SAMPLE)]
data_type_master_key = [rand.randint(0,5) for i in range(0,N_SAMPLE*N_COL_SAMPLE)]
#print(len(data_type_master_key))
is_current = [True]*N_SAMPLE*N_COL_SAMPLE
is_active = [True]*N_SAMPLE*N_COL_SAMPLE
table_mstr_key = [i for i in range(0,N_SAMPLE) for j in range(N_COL_SAMPLE)]


table_field_master_df = pd.DataFrame({
    'table_field_mstr_key': table_field_mstr_key,
    'field_name': field_name,
    'data_type_mstr_key': data_type_master_key,
    'is_active': is_active,
    'is_current':is_current,
    'table_mstr_key':table_mstr_key,
})
table_field_master_df = spark.createDataFrame(table_field_master_df)
table_field_master_df.show()

"""

CREATE TABLE `table_data_master` (
  `table_data_mstr_key` int primary key,
  `record_id` int,
  `data_value` varchar(100),
  `is_active` boolean,
  `is_current` boolean,
  `table_field_mstr_key` int,
  `table_mstr_key` int,
  FOREIGN KEY (`table_mstr_key`) REFERENCES `table_field_master`(`table_field_mstr_key`)
);

"""
N_DATA_SAMPLE=100
table_data_mstr_key = [i for i in range(0,N_SAMPLE*N_COL_SAMPLE*N_DATA_SAMPLE)]
record_id = [i for k in range(0,N_SAMPLE) for i in range(0,N_COL_SAMPLE) for j in range(0,N_DATA_SAMPLE)]
data_value = ['test_value_'+str(i) for i in range(N_SAMPLE*N_COL_SAMPLE*N_DATA_SAMPLE)]
is_current = [True]*N_SAMPLE*N_COL_SAMPLE*N_DATA_SAMPLE
is_active = [True]*N_SAMPLE*N_COL_SAMPLE*N_DATA_SAMPLE
table_field_mstr_key = [j for i in range(0,N_SAMPLE) for j in range(0,N_COL_SAMPLE) for k in range(0,N_DATA_SAMPLE)]
table_table_mstr_key = [i for i in range(0,N_SAMPLE) for j in range(0,N_COL_SAMPLE) for k in range(0,N_DATA_SAMPLE)]

table_data_master_df = pd.DataFrame({
    'table_data_mstr_key': table_data_mstr_key,
    'record_id': record_id,
    'data_value': data_value,
    'is_current': is_current,
    'is_active':is_active,
    'table_field_mstr_key':table_field_mstr_key,
    'table_mstr_key':table_table_mstr_key,
})
table_data_master_df = spark.createDataFrame(table_data_master_df)
table_data_master_df.show(200)


table_master_df.write.format('jdbc').options(
      url='jdbc:mysql://localhost/etl',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='table_master',
      user='hadoop',
      password='root').mode('ignore').save()

data_type_master_df.write.format('jdbc').options(
      url='jdbc:mysql://localhost/etl',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='data_type_master',
      user='hadoop',
      password='root').mode('ignore').save()
table_field_master_df.write.format('jdbc').options(
      url='jdbc:mysql://localhost/etl',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='table_field_master',
      user='hadoop',
      password='root').mode('ignore').save()

table_data_master_df.write.format('jdbc').options(
      url='jdbc:mysql://localhost/etl',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='table_data_master',
      user='hadoop',
      password='root').mode('ignore').save()

