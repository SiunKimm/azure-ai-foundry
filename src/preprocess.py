
print("=== preprocess.py 실행 시작 ===")
import pandas as pd
import json
import numpy as np

def parse_hospital_guidelines(filepath):
    df = pd.read_excel(filepath, header=None)

    structured_data = []
    current_department = None
    current_part = None
    is_in_physician_table = False
    physician_headers = []

    for index, row in df.iterrows():
        # 1. '과'로 끝나는 행을 만나면 새 진료과를 시작합니다. (대분류)
        if pd.notna(row[0]) and str(row[0]).strip().endswith('과'):
            if current_department:
                structured_data.append(current_department)
            current_department = {'department_name': row[0].strip(), 'parts': []}
            current_part = None
            is_in_physician_table = False
            continue

        if current_department is None:
            continue

        # 2. '진료과 공통사항' 행을 처리합니다.
        if pd.notna(row[0]) and '진료과 공통사항' in str(row[0]):
            current_department['department_rules'] = row[2]
            continue

        # 3. 그 외 첫 번째 열에 내용이 있는 행은 모두 새로운 '파트'의 시작으로 간주합니다.
        if pd.notna(row[0]):
            part_name = ' '.join(str(row[0]).split())
            current_part = {'part_name': part_name, 'physician_details': []}
            current_department['parts'].append(current_part)
            is_in_physician_table = False

        if current_part is None:
            continue
        
        # 4. 두 번째 열의 내용을 기반으로 파트의 세부 정보를 처리합니다.
        if pd.notna(row[1]):
            if '공통사항' in str(row[1]):
                current_part['common_rules'] = row[2]
                is_in_physician_table = False
            elif '진료불가' in str(row[1]):
                current_part['unreservable_rules'] = row[2]
                is_in_physician_table = False
            elif '준비사항' in str(row[1]):
                current_part['preparation'] = row[2]
                is_in_physician_table = False
            elif '주치의' in str(row[1]):
                is_in_physician_table = True
                physician_headers = [h.strip() for h in row[1:] if pd.notna(h)]
                continue

        # 5. 주치의 테이블의 데이터 행을 처리합니다.
        if is_in_physician_table and pd.notna(row[1]):
            physician_data = {}
            row_values = row[1:1+len(physician_headers)]
            for header, value in zip(physician_headers, row_values):
                physician_data[header] = value
            current_part['physician_details'].append(physician_data)
        elif is_in_physician_table and pd.isna(row[1]):
             is_in_physician_table = False

    if current_department:
        structured_data.append(current_department)

    return structured_data

def convert_nan_to_none(data):
    """NaN 값을 None으로 변환하는 함수"""
    if isinstance(data, dict):
        return {key: convert_nan_to_none(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [convert_nan_to_none(item) for item in data]
    elif pd.isna(data):
        return None
    else:
        return data

# 코드 실행
file_to_process = 'data/Azure_DataSet.xlsx'
parsed_data = parse_hospital_guidelines(file_to_process)

# NaN 값을 None으로 변환
parsed_data = convert_nan_to_none(parsed_data)

# JSON 파일로 저장
output_file = 'output/preprocessed_data.json'
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, indent=2, ensure_ascii=False)

print(f"데이터가 {output_file}에 저장되었습니다.")

# 콘솔 결과 출력
print(json.dumps(parsed_data, indent=2, ensure_ascii=False))
print("=== preprocess.py 실행 종료 ===")