
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
# import pandas as pd
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')



bb = """
insert overwrite table ods_cn_kone.receipt_detail partition (inc_day = '19950529')
SELECT
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
FROM 
 ods_cn_kone.ar_receipt_detail
"""


print(bb)
bb2 = bb.replace('\n', '')
spark.sql(bb2) 


# left join 
#         dsc_dim.dim_dsc_item_info as IT 
# on      LI.item = IT.sku_code 
# -- and     LI.company = it.wms_company_name 
# and     LI.data_source = IT.data_source
# left join 
#         dsc_dim.dim_dsc_storage_location_info as LO 
# on      LI.location = LO.location_id 
# and     LI.warehouse = LO.warehouse_id 
# and     LI.data_source = LO.data_source