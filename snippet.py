# #!/bin/sh

# for VARIABLE in ods_cn_bose ods_cn_apple_sz ods_cn_apple_sh ods_cn_costacoffee\
#      ods_cn_diadora ods_cn_ferrero ods_cn_fuji ods_cn_hd ods_cn_hp_ljb\
#           ods_cn_hpi ods_cn_hualiancosta ods_cn_jiq ods_cn_kone ods_cn_michelin ods_cn_razer\
#                ods_cn_squibb ods_cn_vzug ods_cn_zebra ods_dbo ods_hk_abbott ods_hk_revlon\
#                     ods_hk_fredperry
# do
# 	hive -e "create TABLE IF not EXISTS $VARIABLE.receipt_header_df AS SELECT *, cast(null as string) as src_inc_day FROM ods_cn_bose.receipt_header where inc_day = '20210826'" 
# done
 
 
# for VARIABLE in ods_cn_bose ods_cn_apple_sz ods_cn_apple_sh ods_cn_costacoffee\
#  ods_cn_diadora ods_cn_ferrero ods_cn_fuji ods_cn_hd ods_cn_hp_ljb ods_cn_hpi\
#  ods_cn_hualiancosta ods_cn_jiq ods_cn_kone ods_cn_michelin ods_cn_razer\
#  ods_cn_squibb ods_cn_vzug ods_cn_zebra ods_dbo ods_hk_abbott ods_hk_revlon\
#  ods_hk_fredperry
# do
#     hive -e "alter table $VARIABLE.receipt_header_df set TBLPROPERTIES ('EXTERNAL' = 'FALSE')"
# 	hive -e "alter table $VARIABLE.receipt_detail_df set TBLPROPERTIES ('EXTERNAL' = 'FALSE')"
#     hive -e "alter table $VARIABLE.shipment_header_df set TBLPROPERTIES ('EXTERNAL' = 'FALSE')"
#     hive -e "alter table $VARIABLE.shipment_detail_df set TBLPROPERTIES ('EXTERNAL' = 'FALSE')"
#     hive -e "drop table if exists $VARIABLE.receipt_header_df"
#     hive -e "drop table if exists $VARIABLE.receipt_detail_df"
#     hive -e "drop table if exists $VARIABLE.shipment_header_df"
#     hive -e "drop table if exists $VARIABLE.shipment_detail_df"
# done
