import pandas as pd
import gc

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

def parse_month_from_filename(filename: str, year: int, config_months: dict) -> int:
    """
    Parse month từ filename theo cấu hình YAML.

    Thứ tự ưu tiên:
    1. fixed_mapping
    2. split_rules
    3. regex_patterns
    4. prefix_extract
    5. fallback lấy số đầu filename
    """

    name_lower = os.path.splitext(filename)[0].lower().strip()
    calculated_month = None

    if year in config_months:
        year_cfg = config_months[year]

        # 1. fixed_mapping phải ưu tiên trước
        if "fixed_mapping" in year_cfg:
            for key, val in year_cfg["fixed_mapping"].items():
                key_lower = str(key).lower().strip()
                if key_lower in name_lower:
                    calculated_month = int(val)
                    break

        # 2. split_rules
        if calculated_month is None and "split_rules" in year_cfg:
            for rule in year_cfg["split_rules"]:
                try:
                    indicator = rule["indicator"]
                    delimiter = rule["delimiter"]
                    index = rule["index"]

                    if indicator in name_lower:
                        sub = name_lower.split(indicator)[1]
                        month_part = sub.split(delimiter)[index].split(".")[0]

                        if month_part.isdigit():
                            calculated_month = int(month_part)
                            break
                except Exception:
                    pass

        # 3. regex_patterns
        if calculated_month is None and "regex_patterns" in year_cfg:
            for pattern in year_cfg["regex_patterns"]:
                match = re.search(pattern, name_lower)
                if match:
                    calculated_month = int(match.group(1))
                    break

        # 4. prefix_extract
        if calculated_month is None and "prefix_extract" in year_cfg:
            prefix = str(year_cfg["prefix_extract"]).lower().strip()

            if prefix in name_lower:
                sub = name_lower.split(prefix)[1]

                if len(sub) >= 2 and sub[:2].isdigit():
                    calculated_month = int(sub[:2])
                else:
                    clean_sub = sub.split(".")[0].replace("-", "")

                    if len(clean_sub) >= 2 and clean_sub[:2].isdigit():
                        calculated_month = int(clean_sub[:2])
                    elif len(clean_sub) >= 1 and clean_sub[0].isdigit():
                        calculated_month = int(clean_sub[0])

    # 5. fallback lấy số ở đầu filename
    if calculated_month is None:
        match = re.search(r"^(\d{1,2})", name_lower)
        if match:
            calculated_month = int(match.group(1))

    if calculated_month is None:
        raise ValueError(f"Không parse được month từ filename={filename}")

    if not (1 <= calculated_month <= 12):
        raise ValueError(f"Month không hợp lệ: {calculated_month}, filename={filename}")

    return calculated_month

def main_func():
    # lấy tất cả các đường dẫn trong bronze
    bucket_name = 'bronze'
    prefix = 'economic_report_excel_files/'

    objects = get_list_files(bucket_name, prefix)

    if objects is None:
        print("Không tìm thấy bất kỳ file báo cáo nào !!!!!!")
        return

    # duyệt qua từng đường dẫn đọc file và trích xuất dữ liệu
    for obj in objects:
        parts = obj.split('/')
        # path: historical/economic_report_excel_files/012011/Bieu-012011.xlsx
        # parts[-2] = '012011', parts[-1] = filename
     
        
        year = int(parts[-2])
        filename = parts[-1]

        try:
            month = parse_month_from_filename(filename, year, config_months)
        except Exception as e:
            print(f"Không parse được tháng cho file: {obj}. Error: {e}")
            continue

        excel_file = get_excel_file(bucket_name, obj)

        if excel_file is None:
            print('Đọc file Excel không thành công')
            continue

        print(f'FILE EXCEL: YEAR : {year}, MONTH = {month}')

        extract_data_from_GDP(excel_file, year, month)

        extract_data_from_International_Ecommerce(excel_file, year, month)

        if not (year == 2014 and month == 3):
            extract_data_from_Invesment(excel_file, year, month)

        extract_data_for_Product_Productivity_fact(excel_file, year, month)

        # Giải phóng bộ nhớ RAM của file hiện tại trước khi xử lý file tiếp theo
        del excel_file
        gc.collect()
    
    print('BẮT ĐẦU TRÍCH XUẤT DỮ LIỆU INVESTMENT BY SECTOR')
    excel_file = get_investment_by_sector_raw_data()
    extract_data_from_Investment_by_Sector(excel_file)

      
    print(f"Tải thành công dữ liệu từ file: tháng: {month} - năm: {year} lên SILVER LAYER")

main_func()
