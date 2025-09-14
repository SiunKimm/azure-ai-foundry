print("=== preprocess.py 실행 시작 ===")
import pandas as pd
import json
import re

def parse_hospital_guidelines_flat(filepath):
    df = pd.read_excel(filepath, header=None)

    flat_data = []
    record_id = 1  # ID 카운터 추가
    
    # --- 필드 이름 매핑 (한글 -> 영문) ---
    # Azure AI Search 필드명 규칙에 맞게 영문으로 변경합니다.
    header_map = {
        "주치의": "physician_name",
        "전문진료": "specialty",
        "특이사항 (회색글씨는 구버전 확인필요)": "notes",
        "예약불가": "unreservable_conditions",
        "진협응급T/O": "emergency_slots",
        "심층진료": "in_depth_treatment",
        "신속질환": "fast_track_disease",
        "중입자치료": "carbon_ion_therapy",
        "보호자대진": "guardian_consultation",
        "외국인진료": "foreign_patient_care"
    }

    current_department_name = None
    current_department_rules = None
    current_part_name = None
    current_part_common_rules = None
    current_part_unreservable_rules = None
    current_part_preparation = None
    
    is_in_physician_table = False
    physician_headers = []

    for index, row in df.iterrows():
        if pd.notna(row[0]) and str(row[0]).strip().endswith('과'):
            current_department_name = row[0].strip()
            current_department_rules = None
            current_part_name = None
            current_part_common_rules = None
            current_part_unreservable_rules = None
            current_part_preparation = None
            is_in_physician_table = False
            continue

        if current_department_name is None:
            continue

        if pd.notna(row[0]) and '진료과 공통사항' in str(row[0]):
            current_department_rules = row[2]
            continue

        if pd.notna(row[0]):
            current_part_name = ' '.join(str(row[0]).split())
            current_part_common_rules = None
            current_part_unreservable_rules = None
            current_part_preparation = None
            is_in_physician_table = False

        if current_part_name is None:
            continue
        
        if pd.notna(row[1]):
            if '공통사항' in str(row[1]):
                current_part_common_rules = row[2]
                is_in_physician_table = False
            elif '진료불가' in str(row[1]):
                current_part_unreservable_rules = row[2]
                is_in_physician_table = False
            elif '준비사항' in str(row[1]):
                current_part_preparation = row[2]
                is_in_physician_table = False
            elif '주치의' in str(row[1]):
                is_in_physician_table = True
                physician_headers = [h.strip() for h in row[1:] if pd.notna(h)]
                continue

        if is_in_physician_table and pd.notna(row[1]):
            physician_data = {}
            row_values = row[1:1+len(physician_headers)]
            for header, value in zip(physician_headers, row_values):
                # --- 핵심 변경: 한글 헤더를 영문으로 변환 ---
                # header_map에서 영문 이름을 찾아 사용합니다. 맵에 없으면 원래 헤더를 그대로 사용합니다.
                english_header = header_map.get(header, header)
                # 혹시 모를 특수문자나 공백을 밑줄(_)로 변경해줍니다.
                english_header = re.sub(r'[^a-zA-Z0-9_]', '_', english_header)
                physician_data[english_header] = value
            
            flat_record = {
                'id': str(record_id),  # 고유 ID 추가
                'department_name': current_department_name,
                'department_rules': current_department_rules,
                'part_name': current_part_name,
                'common_rules': current_part_common_rules,
                'unreservable_rules': current_part_unreservable_rules,
                'preparation': current_part_preparation
            }
            
            flat_record.update(physician_data)
            flat_data.append(flat_record)
            record_id += 1  # ID 카운터 증가

        elif is_in_physician_table and pd.isna(row[1]):
            is_in_physician_table = False

    return flat_data

def convert_nan_to_none(data):
    if isinstance(data, dict):
        return {key: convert_nan_to_none(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_nan_to_none(item) for item in data]
    elif pd.isna(data):
        return None
    else:
        return data

def convert_boolean_fields(data):
    """특정 필드들을 Boolean 값과 상세 정보로 분리하는 함수 (Azure AI Search 최적화)"""
    boolean_fields = [
        "emergency_slots",          # 진협응급T/O
        "in_depth_treatment",       # 심층진료
        "fast_track_disease",       # 신속질환
        "carbon_ion_therapy",       # 중입자치료
        "guardian_consultation",    # 보호자대진
        "foreign_patient_care"      # 외국인진료
    ]
    
    def convert_to_boolean_with_details(field_name, value):
        if value is None:
            return None, None
        
        # 문자열로 변환 후 처리
        str_value = str(value).strip()
        str_value_lower = str_value.lower()
        
        # Boolean False로 간주할 값들  
        false_values = ["x", "불가", "불가능", "절대불가", "no", "n", "false", "0", "아니오"]
        
        if str_value_lower in false_values:
            return False, None
        elif str_value_lower == "" or str_value == "":
            return False, None
        else:
            # 나머지는 모두 True로 간주하고, 원본 텍스트를 details에 저장
            return True, str_value
    
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if key in boolean_fields:
                bool_value, details_value = convert_to_boolean_with_details(key, value)
                result[key] = bool_value
                # details 필드는 항상 생성 (값이 없으면 null)
                result[f"{key}_details"] = details_value
            else:
                result[key] = convert_boolean_fields(value)
        return result
    elif isinstance(data, list):
        return [convert_boolean_fields(item) for item in data]
    else:
        return data

# --- 실행 부분 ---
file_to_process = 'data/Azure_DataSet.xlsx'
parsed_data = parse_hospital_guidelines_flat(file_to_process) 
parsed_data = convert_nan_to_none(parsed_data)
parsed_data = convert_boolean_fields(parsed_data)

output_file = 'output/preprocessed_data_final.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, indent=2, ensure_ascii=False)

print(f"데이터가 {output_file}에 저장되었습니다.")
print("=== preprocess.py 실행 종료 ===")