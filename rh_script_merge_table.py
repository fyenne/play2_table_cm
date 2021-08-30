from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
import argparse
from datetime import date, datetime,timedelta


def run_etl(tables):

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    today = str(date.today()- timedelta(days=1)).replace('-','');print(today)
    names = tables.split(",")
    rh1 = ['insert overwrite table ' + i + 

    ".receipt_header_df partition (inc_day = '" + today + 
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
    ,close_date
    ,source_id
    ,source_name
    ,source_address1
    ,source_address2
    ,source_address3
    ,source_city
    ,source_state
    ,source_postal_code
    ,source_country
    ,source_attention_to
    ,source_phone_num
    ,source_fax_num
    ,source_email_address
    ,priority
    ,carrier
    ,carrier_service
    ,erp_order_num
    ,erp_order_type
    ,bol_num_alpha
    ,license_plate_id
    ,packing_list_id
    ,pro_num_alpha
    ,trailer_id
    ,seal_id
    ,total_containers
    ,total_lines
    ,total_qty
    ,quantity_um
    ,total_weight
    ,weight_um
    ,total_volume
    ,volume_um
    ,total_value
    ,leading_sts
    ,leading_sts_date
    ,leading_sts_failed
    ,trailing_sts
    ,trailing_sts_date
    ,trailing_sts_failed
    ,user_def1
    ,user_def2
    ,user_def3
    ,user_def4
    ,user_def5
    ,user_def6
    ,user_def7
    ,user_def8
    ,user_stamp
    ,process_stamp
    ,date_time_stamp
    ,manually_entered
    ,ship_from
    ,ship_from_address1
    ,ship_from_address2
    ,ship_from_address3
    ,ship_from_city
    ,ship_from_state
    ,ship_from_country
    ,ship_from_postal_code
    ,ship_from_name
    ,ship_from_attention_to
    ,ship_from_email_address
    ,ship_from_phone_num
    ,ship_from_fax_num
    ,scheduled_date_time
    ,arrived_date_time
    ,start_unitize_date_time
    ,end_unitize_date_time
    ,interface_record_id
    ,creation_process_stamp
    ,creation_date_time_stamp
    ,trailer_yard_status_id
    ,upload_interface_batch
    ,in_pre_checkin_ctr_creation
    ,inc_day as src_inc_day   
    from 
    (
    Select 
    internal_receipt_num
    ,warehouse
    ,company
    ,receipt_id
    ,receipt_id_type
    ,receipt_type
    ,receipt_date
    ,close_date
    ,source_id
    ,source_name
    ,source_address1
    ,source_address2
    ,source_address3
    ,source_city
    ,source_state
    ,source_postal_code
    ,source_country
    ,source_attention_to
    ,source_phone_num
    ,source_fax_num
    ,source_email_address
    ,priority
    ,carrier
    ,carrier_service
    ,erp_order_num
    ,erp_order_type
    ,bol_num_alpha
    ,license_plate_id
    ,packing_list_id
    ,pro_num_alpha
    ,trailer_id
    ,seal_id
    ,total_containers
    ,total_lines
    ,total_qty
    ,quantity_um
    ,total_weight
    ,weight_um
    ,total_volume
    ,volume_um
    ,total_value
    ,leading_sts
    ,leading_sts_date
    ,leading_sts_failed
    ,trailing_sts
    ,trailing_sts_date
    ,trailing_sts_failed
    ,user_def1
    ,user_def2
    ,user_def3
    ,user_def4
    ,user_def5
    ,user_def6
    ,user_def7
    ,user_def8
    ,user_stamp
    ,process_stamp
    ,date_time_stamp
    ,manually_entered
    ,ship_from
    ,ship_from_address1
    ,ship_from_address2
    ,ship_from_address3
    ,ship_from_city
    ,ship_from_state
    ,ship_from_country
    ,ship_from_postal_code
    ,ship_from_name
    ,ship_from_attention_to
    ,ship_from_email_address
    ,ship_from_phone_num
    ,ship_from_fax_num
    ,scheduled_date_time
    ,arrived_date_time
    ,start_unitize_date_time
    ,end_unitize_date_time
    ,interface_record_id
    ,creation_process_stamp
    ,creation_date_time_stamp
    ,trailer_yard_status_id
    ,upload_interface_batch
    ,in_pre_checkin_ctr_creation
    ,inc_day
    ,row_number() over(partition by 
    receipt_id, 
    internal_receipt_num 
    order by inc_day desc) as rn 
    from  
    """
    + i + '.receipt_header) as a where rn = 1'
    for i in names
    ]
    
    rh2 = [i.replace('\n', '') for i in rh1]
    [spark.sql(i) for i in rh2]


def main():
    args = argparse.ArgumentParser()
    tables = "ods_cn_bose,ods_cn_apple_sz,ods_cn_apple_sh,ods_cn_costacoffee,ods_cn_diadora,ods_cn_ferrero,ods_cn_fuji,ods_cn_hd,ods_cn_hp_ljb,ods_cn_hpi,ods_cn_hualiancosta,ods_cn_jiq,ods_cn_kone,ods_cn_michelin,ods_cn_razer,ods_cn_squibb,ods_cn_vzug,ods_cn_zebra,ods_dbo,ods_hk_abbott,ods_hk_revlon,ods_hk_fredperry"
    args.add_argument("--tables", help="tables"
                      , default=[tables], nargs="*")

    args_parse = args.parse_args()
    table_names = args_parse.tables[0]
    run_etl(table_names)

if __name__ == '__main__':
    main()
 