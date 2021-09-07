from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
import argparse
from datetime import date, datetime,timedelta


def run_etl(tables, today, yesterday):

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    # today = str(date.today() - timedelta(days=1)).replace('-','');print(today)
    # today_1 = str(date.today() - timedelta(days=2)).replace('-','');print(today_1)
    names = tables.split(",")
    today = today
    yesterday = yesterday
    # rh1 = ['insert overwrite table ' + i + 

    # ".receipt_header_df partition (inc_day = '" + today + """')
    # Select 
    # internal_receipt_num
    # ,warehouse
    # ,company
    # ,receipt_id
    # ,receipt_id_type
    # ,receipt_type
    # ,receipt_date
    # ,close_date
    # ,source_id
    # ,source_name
    # ,source_address1
    # ,source_address2
    # ,source_address3
    # ,source_city
    # ,source_state
    # ,source_postal_code
    # ,source_country
    # ,source_attention_to
    # ,source_phone_num
    # ,source_fax_num
    # ,source_email_address
    # ,priority
    # ,carrier
    # ,carrier_service
    # ,erp_order_num
    # ,erp_order_type
    # ,bol_num_alpha
    # ,license_plate_id
    # ,packing_list_id
    # ,pro_num_alpha
    # ,trailer_id
    # ,seal_id
    # ,total_containers
    # ,total_lines
    # ,total_qty
    # ,quantity_um
    # ,total_weight
    # ,weight_um
    # ,total_volume
    # ,volume_um
    # ,total_value
    # ,leading_sts
    # ,leading_sts_date
    # ,leading_sts_failed
    # ,trailing_sts
    # ,trailing_sts_date
    # ,trailing_sts_failed
    # ,user_def1
    # ,user_def2
    # ,user_def3
    # ,user_def4
    # ,user_def5
    # ,user_def6
    # ,user_def7
    # ,user_def8
    # ,user_stamp
    # ,process_stamp
    # ,date_time_stamp
    # ,manually_entered
    # ,ship_from
    # ,ship_from_address1
    # ,ship_from_address2
    # ,ship_from_address3
    # ,ship_from_city
    # ,ship_from_state
    # ,ship_from_country
    # ,ship_from_postal_code
    # ,ship_from_name
    # ,ship_from_attention_to
    # ,ship_from_email_address
    # ,ship_from_phone_num
    # ,ship_from_fax_num
    # ,scheduled_date_time
    # ,arrived_date_time
    # ,start_unitize_date_time
    # ,end_unitize_date_time
    # ,interface_record_id
    # ,creation_process_stamp
    # ,creation_date_time_stamp
    # ,trailer_yard_status_id
    # ,upload_interface_batch
    # ,in_pre_checkin_ctr_creation
    # ,inc_day as src_inc_day   
    # from 
    # (
    # Select 
    # internal_receipt_num
    # ,warehouse
    # ,company
    # ,receipt_id
    # ,receipt_id_type
    # ,receipt_type
    # ,receipt_date
    # ,close_date
    # ,source_id
    # ,source_name
    # ,source_address1
    # ,source_address2
    # ,source_address3
    # ,source_city
    # ,source_state
    # ,source_postal_code
    # ,source_country
    # ,source_attention_to
    # ,source_phone_num
    # ,source_fax_num
    # ,source_email_address
    # ,priority
    # ,carrier
    # ,carrier_service
    # ,erp_order_num
    # ,erp_order_type
    # ,bol_num_alpha
    # ,license_plate_id
    # ,packing_list_id
    # ,pro_num_alpha
    # ,trailer_id
    # ,seal_id
    # ,total_containers
    # ,total_lines
    # ,total_qty
    # ,quantity_um
    # ,total_weight
    # ,weight_um
    # ,total_volume
    # ,volume_um
    # ,total_value
    # ,leading_sts
    # ,leading_sts_date
    # ,leading_sts_failed
    # ,trailing_sts
    # ,trailing_sts_date
    # ,trailing_sts_failed
    # ,user_def1
    # ,user_def2
    # ,user_def3
    # ,user_def4
    # ,user_def5
    # ,user_def6
    # ,user_def7
    # ,user_def8
    # ,user_stamp
    # ,process_stamp
    # ,date_time_stamp
    # ,manually_entered
    # ,ship_from
    # ,ship_from_address1
    # ,ship_from_address2
    # ,ship_from_address3
    # ,ship_from_city
    # ,ship_from_state
    # ,ship_from_country
    # ,ship_from_postal_code
    # ,ship_from_name
    # ,ship_from_attention_to
    # ,ship_from_email_address
    # ,ship_from_phone_num
    # ,ship_from_fax_num
    # ,scheduled_date_time
    # ,arrived_date_time
    # ,start_unitize_date_time
    # ,end_unitize_date_time
    # ,interface_record_id
    # ,creation_process_stamp
    # ,creation_date_time_stamp
    # ,trailer_yard_status_id
    # ,upload_interface_batch
    # ,in_pre_checkin_ctr_creation
    # ,inc_day
    # ,row_number() over(partition by 
    # receipt_id, 
    # internal_receipt_num 
    # order by inc_day desc, date_time_stamp desc) as rn 
    # from  
    # """
    # + i + '.receipt_header) as a where rn = 1'
    # for i in names
    # ]
    
    # rh2 = [i.replace('\n', '') for i in rh1]
    # print(rh2[0])
    # [spark.sql(i) for i in rh2]

    """
    di part start
    """
    rh1 = ['insert overwrite table ' + i + 
    ".receipt_header_df partition (inc_day = '" + today + """')
    Select 
    a.internal_receipt_num
    ,warehouse
    ,company
    ,a.receipt_id
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
    ,src_inc_day   
    from """  + i +  """.receipt_header_df as a left join (
    select distinct receipt_id, internal_receipt_num 
    from """  + i +  """.receipt_header
    where inc_day = '""" + today + """'
) as b 
on a.receipt_id = b.receipt_id 
and a.internal_receipt_num = b.internal_receipt_num   
where a.inc_day = '""" + yesterday + """' 
and b.receipt_id is null 

union all 

SELECT  
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
    """
    + i + ".receipt_header where inc_day = '" + today + "'"
    for i in names
    ]
    
    rh2 = [i.replace('\n', '') for i in rh1]
    print(rh2[0])
    [spark.sql(i) for i in rh2]

    """
    di part end
    """



def main():
    args = argparse.ArgumentParser()
    tables = "ods_cn_bose,ods_cn_apple_sz,ods_cn_apple_sh,ods_cn_costacoffee,ods_cn_diadora,ods_cn_ferrero,ods_cn_fuji,ods_cn_hd,ods_cn_hp_ljb,ods_cn_hpi,ods_cn_hualiancosta,ods_cn_jiq,ods_cn_kone,ods_cn_michelin,ods_cn_razer,ods_cn_squibb,ods_cn_vzug,ods_cn_zebra,ods_dbo,ods_hk_abbott,ods_hk_revlon,ods_hk_fredperry"
    # default. not passing
    args.add_argument("--tables", help="tables"
                      , default=[tables], nargs="*")
    # passing 
    args.add_argument("--inc_day", help="inc_dayx format: yyyyMMdd"
                      , default=[(datetime.now() - timedelta(days=1)).strftime("%Y%m%d")], 
                      nargs="*")
    args.add_argument("--inc_day_1", help="inc_day-1 day, format: yyyyMMdd"
                      , default=[(datetime.now() - timedelta(days=2)).strftime("%Y%m%d")], 
                      nargs="*")
    args_parse = args.parse_args()
    table_names = args_parse.tables[0]
    today = args_parse.inc_day[0]
    yesterday = args_parse.inc_day_1[0]
    run_etl(table_names, today, yesterday)

if __name__ == '__main__':
    main()
 