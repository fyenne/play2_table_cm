# test for runnnign

from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
import argparse

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')


from datetime import date, datetime,timedelta
today = str(date.today()- timedelta(days=1)).replace('-','');print(today)

# str(datetime.now().year * 100 + month_num)
def main():
    args = argparse.ArgumentParser()
    month_num = datetime.now().month - 1;month_num
    default_month = ""
    if month_num < 0:
        default_month = str((datetime.now().year - 1) * 100 + 12 + month_num)
    else:
        default_month = str(datetime.now().year * 100 + month_num)
    args.add_argument("--inc_day", help="start month for refresh data, format: yyyyMM"
                      , default=[default_month], nargs="*")
    # args.add_argument("--end_date", help="end month for refresh data, format: yyyyMM"
    #                   , default=[default_month], nargs="*")

    args_parse = args.parse_args()
    inc_day = args_parse.inc_day[0]
    # end_date = args_parse.end_date[0]

    return inc_day

if __name__ == '__main__':
    main()



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

"""
第三部分,  drop duplicates and insert overwrite. 
"""
rh1 = ['insert overwrite table ' + i + 

".receipt_header_df partition (inc_day = '" + inc_day + 
"""
')
Select 
internal_receipt_num
,warehouse
,company
,receipt_id
,receipt_id_type
,receipt_type
,receipt_date
,close_date""" for i in names]

print(rh1)