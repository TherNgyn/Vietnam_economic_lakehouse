import pyspark.pandas as pd
import gc
import yaml
from minio_funcs import *
from reuse_function import *
from Load_data_to_table import *

from gdp_extractor import extract_data_from_GDP
from international_ecommerce_extractor import extract_data_from_International_Ecommerce
from investment_extractor import extract_data_from_Invesment
from investment_by_sector_extractor import extract_data_from_Investment_by_Sector
from product_productivity_extractor import extract_data_for_Product_Productivity_fact

current_dir = os.path.dirname(os.path.abspath(__file__))
yaml_path = os.path.join(current_dir, "month_year.yaml")

with open(yaml_path, "r", encoding="utf-8") as f:
    config_months = yaml.safe_load(f)
    
def main_func():
    bucket_name = 'bronze'
    prefix = 'historical/economic_report_excel_files/'

    objects = get_list_files(bucket_name, prefix)

    if not objects:
        print("Không tìm thấy bất kỳ file báo cáo nào !!!!!!")
        return

    processed = 0

    for obj in objects:
        parts = obj.split('/')
        # path: historical/economic_report_excel_files/012011/Bieu-012011.xlsx
        # parts[-2] = '012011', parts[-1] = filename
     
        year = int(parts[-2])                    # 2011
        filename = parts[-1]                     # 'Bieu-012011.xlsx'
        name_part = filename.replace('Bieu-', '').replace('.xlsx', '')  # '012011'
        part_str = name_part[:2]

        part_str = name_part[:2]
    
        if part_str.strip().isdigit() and 1 <= int(part_str) <= 12:
            month = int(part_str)
        else:
            calculated_month = None
            name_lower = name_part.lower()
            
            if year in config_months:
                year_cfg = config_months[year]
                
                if "fixed_mapping" in year_cfg:
                    for key, val in year_cfg["fixed_mapping"].items():
                        if key in name_lower:
                            calculated_month = val
                            break
                
                if calculated_month is None and "split_rules" in year_cfg:
                    for rule in year_cfg["split_rules"]:
                        if rule["indicator"] in name_lower:
                            sub = name_lower.split(rule["indicator"])[1]
                            month_part = sub.split(rule["delimiter"])[rule["index"]].split(".")[0]
                            if month_part.isdigit():
                                calculated_month = int(month_part)
                                break
                
                # Xử lý Regex thông minh bóc tách được cả số đứng sau chữ 't' (T10, T01, T2...)
                if calculated_month is None and "regex_patterns" in year_cfg:
                    for pattern in year_cfg["regex_patterns"]:
                        match = re.search(pattern, name_lower)
                        if match:
                            calculated_month = int(match.group(1))
                            break
                            
                if calculated_month is None and "prefix_extract" in year_cfg:
                    prefix = year_cfg["prefix_extract"]
                    if prefix in name_lower:
                        sub = name_lower.split(prefix)[1]
                        if sub[:2].isdigit():
                            calculated_month = int(sub[:2])
                        else:
                            clean_sub = sub.split(".")[0].replace("-", "")
                            if len(clean_sub) >= 2 and clean_sub[:2].isdigit():
                                calculated_month = int(clean_sub[:2])
                            elif clean_sub[0].isdigit():
                                calculated_month = int(clean_sub[0])
                                
            if calculated_month is not None:
                month = calculated_month
            else:
                # Code fallback gốc của bạn phòng hờ tất cả các trường hợp
                if "." in part_str:
                    month = int(part_str.replace(".", "").strip())
                elif part_str.strip().isdigit():
                    month = int(part_str)
                elif part_str[0].isdigit():
                    month = int(part_str[0])
                else:
                    month = int(name_part[:2])
        excel_file = get_excel_file(bucket_name, obj)

        if excel_file is None:
            print('Đọc file Excel không thành công')
            continue

        print(f'FILE EXCEL: YEAR : {year}, MONTH = {month}')

        extract_data_from_GDP(excel_file, year, month)
        extract_data_from_International_Ecommerce(excel_file, year, month)

        if not (year == 2014 and month == 3):
            extract_data_from_Invesment(excel_file, year, month)

        extract_data_from_Investment_by_Sector(excel_file, year, month)
        extract_data_for_Product_Productivity_fact(excel_file, year, month)

        del excel_file
        spark.catalog.clearCache()
        gc.collect()

        processed += 1
        print(f"Tải thành công: tháng {month} - năm {year}")

    print(f"Hoàn tất: đã xử lý {processed}/{len(objects)} file lên SILVER LAYER")

main_func()