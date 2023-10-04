from pyspark.sql import SparkSession
from pyspark.sql.functions import *

"""
      create table documents(
        created_at string,
        id long,
        document_type int
      ) using parquet partitioned by (document_type)
      Data sample (documents.show):
      +-------------------+---+-------------+
      |         created_at| id|document_type|
      +-------------------+---+-------------+
      |2016-01-01 00:00:00|  8|            1|
      |...................|...|.............|
      |2020-01-01 00:00:00|  9|            2|
    
    documents.count() = 100000000
"""

spark = SparkSession.builder.appName("Test").getOrCreate()

documents = spark.table("documents").select("document_type", "created_at", "id")
types = spark.createDataFrame([(1,), (2,), (3,), (4,), (5,)], ["doc_type"])

docTypesCreatedBetweenJanAndApr2017 = documents \
    .join(types, documents.document_type == types.doc_type) \
    .where(substring(documents.created_at, 1, 4) == "2017") \
    .groupBy(documents.document_type) \
    .agg(min(documents.created_at).alias("min_created")) \
    .where(col('min_created') < "2017-05-01 00:00:00")

uniqueDocTypes = len(set(docTypesCreatedBetweenJanAndApr2017.select('document_type').collect()))

print("Unique document types: {}".format(uniqueDocTypes))

docTypesCreatedBetweenJanAndApr2017.write.parquet("some parquet path")

"""
docTypesCreatedBetweenJanAndApr2017.explain
   
    == Physical Plan ==
*(3) Filter (isnotnull(min_created#63) && (min_created#63 < 2017-05-01 00:00:00))
+- SortAggregate(key=[document_type#19], functions=[min(created_at#17)])
   +- *(2) Sort [document_type#19 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(document_type#19, 200)
         +- SortAggregate(key=[document_type#19], functions=[partial_min(created_at#17)])
            +- *(1) Sort [document_type#19 ASC NULLS FIRST], false, 0
               +- *(1) Project [document_type#19, created_at#17]
                  +- *(1) BroadcastHashJoin [document_type#19], [doc_type#37], Inner, BuildRight
                     :- *(1) Project [document_type#19, created_at#17]
                     :  +- *(1) Filter (isnotnull(created_at#17) && (substring(created_at#17, 1, 4) = 2017))
                     :     +- *(1) FileScan parquet default.documents[created_at#17,document_type#19] 
                                   PartitionFilters: [isnotnull(document_type#19)], PushedFilters: [IsNotNull(created_at)]
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
                        +- LocalTableScan [doc_type#37]
"""
