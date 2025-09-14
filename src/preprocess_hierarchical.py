import pandas as pd
import json
import re

def parse_hospital_guidelines_smart_flat(filepath):
    """
    엑셀 파일을 파싱하여 모든 계층 정보를 포함하는 단일 플랫 파일(JSON)을 생성합니다.
    - 파트 정보가 섞이는 버그를 수정한 로직을 포함합니다.
    - 각 주치의 레코드는 자신이 속한 과와 파트의 모든 정보를 가집니다.
    """
    df = pd.read_excel(filepath, header=None)

    flat_data = []
    record_id = 1
    
    header_map = {
        "주치의": "physician_name", "전문진료": "specialty",
        "특이사항 (회색글씨는 구버전 확인필요)": "notes", "예약불가": "unreservable_conditions",
        "진협응급T/O": "emergency_slots", "심층진료": "in_depth_treatment",
        "신속질환": "fast_track_disease", "중입자치료": "carbon_ion_therapy",
        "보호자대진": "guardian_consultation", "외국인진료": "foreign_patient_care"
    }

    # 현재 처리중인 컨텍스트 정보를 저장하는 변수
    current_department_info = {}
    current_part_info = {}
    
    is_in_physician_table = False
    physician_headers = []

    for index, row in df.iterrows():
        # 1. '과' 정보 처리
        if pd.notna(row[0]) and str(row[0]).strip().endswith('과'):
            current_department_info = {'department_name': str(row[0]).strip(), 'department_rules': None}
            current_part_info = {}  # 과가 바뀌면 파트 정보 초기화
            is_in_physician_table = False
            continue

        if not current_department_info: continue

        # 2. '진료과 공통사항' 처리
        if pd.notna(row[0]) and '진료과 공통사항' in str(row[0]):
            current_department_info['department_rules'] = row[2]
            continue

        # 3. '파트' 정보 처리 (핵심 로직 수정)
        if not is_in_physician_table and pd.notna(row[0]):
            # 새로운 파트 이름이 나오면, 파트 정보 객체를 새로 만듦
            current_part_info = {
                'part_name': ' '.join(str(row[0]).split()),
                'common_rules': None,
                'unreservable_rules': None,
                'preparation': None
            }
            continue

        if not current_part_info: continue

        # 4. '파트'의 세부 규칙 및 '주치의' 테이블 시작점 처리
        if pd.notna(row[1]):
            rule_type = str(row[1]).strip()
            # 현재 파트 정보 객체에 규칙들을 채워넣음
            if '공통사항' in rule_type:
                current_part_info['common_rules'] = row[2]
                is_in_physician_table = False
            elif '진료불가' in rule_type:
                current_part_info['unreservable_rules'] = row[2]
                is_in_physician_table = False
            elif '준비사항' in rule_type:
                current_part_info['preparation'] = row[2]
                is_in_physician_table = False
            elif '주치의' in rule_type:
                is_in_physician_table = True
                physician_headers = [h.strip() for h in row[1:] if pd.notna(h)]
                continue

        # 5. '주치의' 테이블 데이터 행 처리
        if is_in_physician_table and pd.notna(row[1]):
            physician_data = {}
            row_values = row[1:1+len(physician_headers)]
            for header, value in zip(physician_headers, row_values):
                english_header = header_map.get(header, header)
                physician_data[english_header] = value
            
            # 최종 레코드 생성: 과 + 파트 + 주치의 정보 결합
            final_record = {
                'id': str(record_id),
                **current_department_info,
                **current_part_info,
                **physician_data
            }
            flat_data.append(final_record)
            record_id += 1
        
        elif is_in_physician_table and pd.isna(row[1]):
            is_in_physician_table = False

    return flat_data

# --- Helper Functions (기존과 동일) ---
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
    boolean_fields = [
        "emergency_slots", "in_depth_treatment", "fast_track_disease",
        "carbon_ion_therapy", "guardian_consultation", "foreign_patient_care"
    ]
    
    def convert_to_boolean_with_details(value):
        if value is None: return None, None
        str_value = str(value).strip().lower()
        false_values = ["x", "불가", "불가능", "절대불가", "no", "n", "false", "0", "아니오", ""]
        is_true = str_value not in false_values
        return is_true, str(value) if is_true else None

    processed_data = []
    for item in data:
        new_item = {}
        for key, value in item.items():
            if key in boolean_fields:
                bool_val, details_val = convert_to_boolean_with_details(value)
                new_item[key] = bool_val
                new_item[f"{key}_details"] = details_val
            else:
                new_item[key] = value
        processed_data.append(new_item)
    return processed_data

# --- 실행 부분 ---
print("=== 개선된 단일 파일 전처리기 실행 시작 ===")
file_to_process = 'data/Azure_DataSet.xlsx'
parsed_data = parse_hospital_guidelines_smart_flat(file_to_process)
parsed_data = convert_nan_to_none(parsed_data)
parsed_data = convert_boolean_fields(parsed_data)

output_file = 'output/hospital_data_combined.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, indent=2, ensure_ascii=False)

print(f"모든 정보가 통합된 데이터가 {output_file}에 저장되었습니다.")
print("=== 실행 종료 ===")
