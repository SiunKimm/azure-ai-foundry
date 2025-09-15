print("=== preprocess.py 실행 시작 ===")
import pandas as pd
import json
import re

def parse_hospital_guidelines_flat(filepath):
    """
    병원 가이드라인 Excel 파일을 읽어서 플랫 구조의 데이터로 변환합니다.
    각 주치의 정보를 개별 레코드로 분리하여 Azure AI Search에 최적화된 형태로 변환합니다.
    """
    # Excel 파일을 헤더 없이 읽어옵니다
    df = pd.read_excel(filepath, header=None)

    flat_data = []
    record_id = 1  # 각 레코드의 고유 ID를 위한 카운터
    
    # --- 필드 이름 매핑 (한글 -> 영문) ---
    # Azure AI Search 필드명 규칙에 맞게 한글 필드명을 영문으로 변환합니다.
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

    # 데이터 파싱을 위한 상태 변수들
    current_department_name = None          # 현재 처리 중인 진료과명
    current_department_rules = None         # 진료과 공통 규칙
    current_part_name = None                # 현재 처리 중인 진료 부문명
    current_part_common_rules = None        # 부문별 공통 규칙
    current_part_unreservable_rules = None  # 부문별 예약불가 규칙
    current_part_preparation = None         # 부문별 준비사항
    
    is_in_physician_table = False           # 주치의 테이블 처리 중인지 여부
    physician_headers = []                  # 주치의 테이블 헤더 정보

    # DataFrame의 각 행을 순회하면서 데이터를 파싱합니다
    for index, row in df.iterrows():
        # 진료과명 처리 ('과'로 끝나는 셀)
        if pd.notna(row[0]) and str(row[0]).strip().endswith('과'):
            current_department_name = row[0].strip()
            # 새로운 진료과 시작시 모든 상태 초기화
            current_department_rules = None
            current_part_name = None
            current_part_common_rules = None
            current_part_unreservable_rules = None
            current_part_preparation = None
            is_in_physician_table = False
            continue

        # 진료과가 설정되지 않은 경우 스킵
        if current_department_name is None:
            continue

        # 진료과 공통사항 처리
        if pd.notna(row[0]) and '진료과 공통사항' in str(row[0]):
            current_department_rules = row[2]
            continue

        # 진료 부문명 처리 (첫 번째 컬럼에 값이 있는 경우)
        if pd.notna(row[0]):
            current_part_name = ' '.join(str(row[0]).split())
            # 새로운 부문 시작시 부문 관련 상태 초기화
            current_part_common_rules = None
            current_part_unreservable_rules = None
            current_part_preparation = None
            is_in_physician_table = False

        # 부문이 설정되지 않은 경우 스킵
        if current_part_name is None:
            continue
        
        # 두 번째 컬럼의 특수 항목들 처리
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
                # 주치의 테이블 시작 - 헤더 행 처리
                is_in_physician_table = True
                physician_headers = [h.strip() for h in row[1:] if pd.notna(h)]
                continue

        # 주치의 데이터 행 처리
        if is_in_physician_table and pd.notna(row[1]):
            physician_data = {}
            row_values = row[1:1+len(physician_headers)]
            # 각 헤더에 대응하는 값을 매핑
            for header, value in zip(physician_headers, row_values):
                # 한글 헤더를 영문으로 변환 (매핑에 없으면 원래 헤더 사용)
                english_header = header_map.get(header, header)
                # 특수문자나 공백을 밑줄로 변경하여 필드명 정규화
                english_header = re.sub(r'[^a-zA-Z0-9_]', '_', english_header)
                physician_data[english_header] = value
            
            # 플랫 구조의 레코드 생성
            flat_record = {
                'id': str(record_id),  # 고유 ID 추가
                'department_name': current_department_name,             # 진료과명
                'department_rules': current_department_rules,           # 진료과 공통 규칙
                'part_name': current_part_name,                         # 진료 부문명
                'common_rules': current_part_common_rules,              # 부문 공통 규칙
                'unreservable_rules': current_part_unreservable_rules,  # 예약불가 규칙
                'preparation': current_part_preparation                 # 준비사항
            }
            
            # 주치의 데이터를 레코드에 병합
            flat_record.update(physician_data)
            flat_data.append(flat_record)
            record_id += 1  # ID 카운터 증가

        # 주치의 테이블이 끝나는 지점 감지 (빈 행)
        elif is_in_physician_table and pd.isna(row[1]):
            is_in_physician_table = False

    return flat_data

def convert_nan_to_none(data):
    """
    pandas의 NaN 값을 None으로 변환합니다.
    JSON 직렬화에서 NaN은 지원되지 않으므로 None으로 변환이 필요합니다.
    """
    if isinstance(data, dict):
        return {key: convert_nan_to_none(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_nan_to_none(item) for item in data]
    elif pd.isna(data):
        return None
    else:
        return data

def convert_boolean_fields(data):
    """
    특정 필드들을 Boolean 값과 상세 정보로 분리하는 함수
    Azure AI Search에서 검색 최적화를 위해 Boolean 필드와 텍스트 상세 정보를 분리합니다.
    """
    # Boolean으로 변환할 대상 필드들 (영문 필드명)
    boolean_fields = [
        "emergency_slots",          # 진협응급T/O
        "in_depth_treatment",       # 심층진료
        "fast_track_disease",       # 신속질환
        "carbon_ion_therapy",       # 중입자치료
        "guardian_consultation",    # 보호자대진
        "foreign_patient_care"      # 외국인진료
    ]
    
    def convert_to_boolean_with_details(field_name, value):
        """
        개별 필드 값을 Boolean과 상세 정보로 분리합니다.
        
        Args:
            field_name: 필드명 (현재는 미사용)
            value: 변환할 원본 값
            
        Returns:
            tuple: (boolean_value, details_text)
        """
        if value is None:
            return None, None
        
        # 문자열로 변환 후 처리
        str_value = str(value).strip()
        str_value_lower = str_value.lower()
        
        # False로 간주할 값들 정의  
        false_values = ["x", "불가", "불가능", "절대불가", "no", "n", "false", "0", "아니오"]
        
        if str_value_lower in false_values:
            return False, None
        elif str_value_lower == "" or str_value == "":
            return False, None
        else:
            # 나머지는 모두 True로 간주하고, 원본 텍스트를 details에 저장
            return True, str_value
    
    # 재귀적으로 데이터 구조를 처리합니다
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if key in boolean_fields:
                # Boolean 필드인 경우 값과 상세 정보로 분리
                bool_value, details_value = convert_to_boolean_with_details(key, value)
                result[key] = bool_value
                # details 필드는 항상 생성 (값이 없으면 null)
                result[f"{key}_details"] = details_value
            else:
                # 일반 필드인 경우 재귀 처리
                result[key] = convert_boolean_fields(value)
        return result
    elif isinstance(data, list):
        return [convert_boolean_fields(item) for item in data]
    else:
        return data

# === 메인 실행 부분 ===

# 처리할 Excel 파일 경로
file_to_process = 'data/Azure_DataSet.xlsx'

# 1단계: Excel 파일을 플랫 구조로 파싱
parsed_data = parse_hospital_guidelines_flat(file_to_process) 

# 2단계: NaN 값을 None으로 변환 (JSON 직렬화 준비)
parsed_data = convert_nan_to_none(parsed_data)

# 3단계: 특정 필드들을 Boolean + 상세정보 형태로 변환
parsed_data = convert_boolean_fields(parsed_data)

# 4단계: 최종 JSON 파일로 저장
output_file = 'output/preprocessed_data_final.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, indent=2, ensure_ascii=False)

print(f"데이터가 {output_file}에 저장되었습니다.")
print("=== preprocess.py 실행 종료 ===")