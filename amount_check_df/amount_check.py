

from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
# import pandas as pd
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')


names = ['ods_cn_bose'
,'ods_cn_apple_sz'
,'ods_cn_apple_sh'
,'ods_cn_costacoffee'
,'ods_cn_diadora'
,'ods_cn_ferrero'
,'ods_cn_fuji'
,'ods_cn_hd'
,'ods_cn_hp_ljb'
,'ods_cn_hpi'
,'ods_cn_hualiancosta'
,'ods_cn_jiq'
,'ods_cn_kone'
,'ods_cn_michelin'
,'ods_cn_razer'
,'ods_cn_squibb'
,'ods_cn_vzug'
,'ods_cn_zebra'
,'ods_dbo'
,'ods_hk_abbott'
,'ods_hk_revlon'
,'ods_hk_fredperry'
] 
 

rh = [
    "SELECT  count(0) as ss, '" + i +"' as data_source FROM " + i + 
    """.receipt_header where inc_day = '19950529'
    union all
    select count(0) as ss, '"""  + i +"_ar' as data_source FROM "  + i + '.ar_receipt_header'
    for i in names]

rh2 = [i.replace('\n', '') for i in rh]

print(rh2[1])
# [spark.sql(i) for i in rh2]

for i in rh2:
    spark.sql(i).show()
    print(i)


rd = [
    "SELECT  count(0) as ss, '" + i +"' as data_source FROM " + i + 
    """.receipt_detail where inc_day = '19950529'
    union all
    select count(0) as ss, '"""  + i +"_ar' as data_source FROM "  + i + '.ar_receipt_detail'
    for i in names]
rd2 = [i.replace('\n', '') for i in rd]
rd2
# print(rh2)
# [spark.sql(i) for i in rd2]

for i in rd2:
    spark.sql(i).show()
    print(i)


sh = [
    "SELECT  count(0) as ss, '" + i +"' as data_source FROM " + i + 
    """.shipment_header where inc_day = '19950529'
    union all
    select count(0) as ss, '"""  + i +"_ar' as data_source FROM "  + i + '.ar_shipment_header'
    for i in names]
sh2 = [i.replace('\n', '') for i in sh]
# print(rh2)
# [spark.sql(i) for i in rd2]

for i in sh2:
    spark.sql(i).show()
    print(i)



sd = [
    "SELECT  count(0) as ss, '" + i +"' as data_source FROM " + i + 
    """.shipment_detail where inc_day = '19950529'
    union all
    select count(0) as ss, '"""  + i +"_ar' as data_source FROM "  + i + '.ar_shipment_detail'
    for i in names]
sd2 = [i.replace('\n', '') for i in sd]
# print(rh2)
# [spark.sql(i) for i in rd2]

for i in sd2:
    spark.sql(i).show()
    print(i)


 
"""
## receipt header archive = 0
 
ods_cn_hualiancosta~
 
ods_cn_vzug~
ods_cn_razer~
"""
"""
## receipt detail table  

# ods_cn_hualiancosta ~
ods_cn_kone ~
# ods_cn_razer~
# ods_cn_vzug~
"""
"""
## shipment header table  
 
ods_cn_hp_ljb~
ods_cn_hualiancosta~
ods_cn_razer~
ods_cn_vzug~
 
"""
"""
## shipment d table  
ods_cn_hd ~
ods_cn_hp_ljb ~
ods_cn_hualiancosta ~
ods_cn_razer~
ods_cn_vzug~
ods_hk_fredperry~
"""