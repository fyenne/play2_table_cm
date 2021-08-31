from  pyspark.sql  import  SparkSession
from  pyspark.sql.functions  import  *  
from  datetime  import  date,  datetime,timedelta
import argparse


def run_etl(tables):

    spark  =  SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set('spark.sql.sources.partitionOverwriteMode',  'dynamic')
    today  =  str(date.today()-  timedelta(days=1)).replace('-','');print(today)
    names = tables.split(",")
        
    rd1 = ['insert overwrite table ' + i + 

    ".receipt_detail_df partition (inc_day = '" + today + 

    """
    ') 
    Select
        internal_receipt_line_num,
        locating_rule,
        warehouse,
        internal_receipt_num,
        receipt_id,
        item,
        item_desc,
        item_class,
        lot,
        company,
        total_qty,
        quantity_um,
        open_qty,
        erp_order_num,
        erp_order_type,
        erp_order_line_num,
        expiration_date_time,
        manufactured_date_time,
        item_weight,
        weight_um,
        item_length,
        item_width,
        item_height,
        item_dimension_um,
        volume_um,
        item_division,
        item_department,
        value,
        item_size,
        item_color,
        item_style,
        launch_num,
        put_loc,
        user_def1,
        user_def2,
        user_def3,
        user_def4,
        user_def5,
        user_def6,
        user_def7,
        user_def8,
        user_stamp,
        process_stamp,
        date_time_stamp,
        put_zone,
        item_list_price,
        item_net_price,
        put_list_num,
        manually_entered,
        lot_controlled,
        serial_num_reqd,
        catch_weight_reqd,
        source_id,
        hazardous_code,
        total_weight,
        status_flow_name,
        expected_inv_sts,
        ship_from,
        conversion_qty,
        conversion_um,
        priority,
        customer_order_num,
        receipt_date,
        conversion_height,
        conversion_width,
        conversion_length,
        conversion_weight,
        original_total_quantity,
        interface_record_id,
        item_category1,
        item_category2,
        item_category3,
        item_category4,
        item_category5,
        item_category6,
        item_category7,
        item_category8,
        item_category9,
        item_category10,
        inventory_sts,
        purchase_order_detail_id,
        purchase_order_id,
        purchase_order_line_number
        ,inc_day as src_inc_day 
    from(
    select 
        internal_receipt_line_num,
        locating_rule,
        warehouse,
        internal_receipt_num,
        receipt_id,
        item,
        item_desc,
        item_class,
        lot,
        company,
        total_qty,
        quantity_um,
        open_qty,
        erp_order_num,
        erp_order_type,
        erp_order_line_num,
        expiration_date_time,
        manufactured_date_time,
        item_weight,
        weight_um,
        item_length,
        item_width,
        item_height,
        item_dimension_um,
        volume_um,
        item_division,
        item_department,
        value,
        item_size,
        item_color,
        item_style,
        launch_num,
        put_loc,
        user_def1,
        user_def2,
        user_def3,
        user_def4,
        user_def5,
        user_def6,
        user_def7,
        user_def8,
        user_stamp,
        process_stamp,
        date_time_stamp,
        put_zone,
        item_list_price,
        item_net_price,
        put_list_num,
        manually_entered,
        lot_controlled,
        serial_num_reqd,
        catch_weight_reqd,
        source_id,
        hazardous_code,
        total_weight,
        status_flow_name,
        expected_inv_sts,
        ship_from,
        conversion_qty,
        conversion_um,
        priority,
        customer_order_num,
        receipt_date,
        conversion_height,
        conversion_width,
        conversion_length,
        conversion_weight,
        original_total_quantity,
        interface_record_id,
        item_category1,
        item_category2,
        item_category3,
        item_category4,
        item_category5,
        item_category6,
        item_category7,
        item_category8,
        item_category9,
        item_category10,
        inventory_sts,
        purchase_order_detail_id,
        purchase_order_id,
        purchase_order_line_number
        ,inc_day
        ,row_number() over(partition by 
        receipt_id,  
        internal_receipt_num,
        internal_receipt_line_num
        order by inc_day desc) as rn 
    from  
    """
    + i + '.receipt_detail) as a where rn = 1'
    for i in names
    ]


    rd2 = [i.replace('\n', '') for i in rd1]
    [spark.sql(i) for i in rd2]



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
