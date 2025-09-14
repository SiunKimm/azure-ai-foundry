# preprocess.py
from __future__ import annotations

print("=== preprocess.py 실행 시작 ===")
import pandas as pd
import json
import re
import os
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Tuple, Any, Dict, List

INPUT_XLSX = "data/Azure_DataSet.xlsx"
OUT_DIR = Path("output")
OUT_JSON = OUT_DIR / "preprocessed_data_final.json"   # JSON 배열
OUT_JSONL = OUT_DIR / "preprocessed_data_final.jsonl" # JSONL (권장)

# ---------- 유틸: 텍스트/타입 정규화 ----------
def norm_txt(x: Any) -> Optional[str]:
    if pd.isna(x) or x is None:
        return None
    s = str(x)
    s = re.sub(r"[ \t]+", " ", s)              # 다중 공백 -> 한 칸
    s = re.sub(r"\u00A0+", " ", s)             # non-breaking space
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s or None

def split_list(s: Optional[str]) -> Optional[List[str]]:
    if not s:
        return None
    # 쉼표/개행/세미콜론/중점 등으로 분리
    parts = re.split(r"[,\n;·•]+", s)
    out = [p.strip() for p in parts if p and p.strip()]
    return out or None

BOOL_TRUE = {"o", "0", "true", "t", "yes", "y", "가능", "가능함", "가능합니다", "가능합니다.", "가능요"}
BOOL_FALSE = {"x", "false", "f", "no", "n", "불가", "절대불가", "금지"}

def parse_bool_with_notes(s: Optional[str]) -> Tuple[Optional[bool], Optional[str]]:
    """문자열에서 불리언을 추출하고, 나머지 부가 설명은 notes로 남긴다."""
    if not s:
        return None, None
    raw = norm_txt(s)
    # 첫 줄만 보고 boolean을 추정
    first = raw.split("\n", 1)[0].strip()
    # 괄호/대괄호/별표 제거 후 비교
    key = re.sub(r"[\[\]()*]", "", first).strip().lower()
    if key in BOOL_TRUE:
        return True, (raw if raw != first else None)
    if key in BOOL_FALSE:
        return False, (raw if raw != first else None)
    # '마지막 시간', '정원 초과여도' 등은 True 성격 메모인 경우가 많음 -> 판단 보류하고 notes만
    return None, raw

def parse_gender_year(s: Optional[str]) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    """
    예시:
      'A (남,96졸)' -> gender=남, graduation_year=1996
      'H (여,91생)' -> gender=여, birth_year=1991
      'D (남,  )'   -> gender=남
    """
    if not s:
        return None, None, None
    m = re.search(r"\((?P<gender>남|여)?\s*,?\s*(?P<yy>\d{2})?\s*(?P<tag>졸|생)?\)", s)
    gender = m.group("gender") if m else None
    grad_year = birth_year = None
    if m and m.group("yy"):
        yy = int(m.group("yy"))
        # 00~29 -> 2000대, 그 외 -> 1900대로 가정
        century = 2000 if yy <= 29 else 1900
        year = century + yy
        if m.group("tag") == "졸":
            grad_year = year
        elif m.group("tag") == "생":
            birth_year = year
    return gender, grad_year, birth_year

def parse_year_month_range(s: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    '25.8~25.12' 등 가용기간을 ISO 날짜로 변환
    시작은 해당월 1일, 종료는 해당월 말일(단순히 28/29/30/31은 여기선 28로 보수적 처리)
    """
    if not s:
        return None, None
    m = re.search(r"(?P<syy>\d{2})\.(?P<sm>\d{1,2})\s*~\s*(?P<eyy>\d{2})\.(?P<em>\d{1,2})", s)
    if not m:
        return None, None
    def to_iso(yy, mm, end=False):
        yy = int(yy); mm = int(mm)
        year = (2000 if yy <= 29 else 1900) + yy
        day = 28 if end else 1
        return f"{year:04d}-{mm:02d}-{day:02d}"
    start = to_iso(m.group("syy"), m.group("sm"), end=False)
    end   = to_iso(m.group("eyy"), m.group("em"), end=True)
    return start, end

def make_id(dept: Optional[str], part: Optional[str], name: Optional[str]) -> str:
    basis = "|".join([dept or "", part or "", name or ""]).lower()
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()

def extract_dept_code(name: Optional[str]) -> Optional[str]:
    # 'NM. 신경과', 'NS. 신경외과' 등에서 접두 코드 추출
    if not name:
        return None
    m = re.match(r"([A-Z]{2})\.\s*", name)
    return m.group(1) if m else None

# ---------- 파서 본체 ----------
HEADER_MAP = {
    "주치의": "physician_name",
    "전문진료": "specialty",
    "특이사항 (회색글씨는 구버전 확인필요)": "notes",
    "예약불가": "unreservable_conditions",
    "진협응급T/O": "emergency_slots",
    "심층진료": "in_depth_treatment",
    "신속질환": "fast_track_disease",
    "중입자치료": "carbon_ion_therapy",
    "보호자대진": "guardian_consultation",
    "외국인진료": "foreign_patient_care",
}

PART_LIKE_PAT = re.compile(r"(파트|센터|클리닉|본관|수면건강센터|뇌신경센터)")

def parse_hospital_guidelines_flat(filepath: str) -> List[Dict[str, Any]]:
    df = pd.read_excel(filepath, header=None, engine="openpyxl")

    flat: List[Dict[str, Any]] = []
    dept_name = dept_rules = None
    part_name = part_common = part_unreserv = part_prep = None
    in_phys_table = False
    phys_headers: List[str] = []

    for idx, row in df.iterrows():
        c0, c1 = row[0], row[1]
        s0, s1 = norm_txt(c0), norm_txt(c1)

        # 1) 부서행
        if s0 and s0.endswith("과"):
            dept_name = s0
            dept_rules = None
            part_name = part_common = part_unreserv = part_prep = None
            in_phys_table = False
            continue

        if not dept_name:
            continue

        # 2) 부서 공통사항
        if s0 and "진료과 공통사항" in s0:
            dept_rules = norm_txt(row[2])
            continue

        # 3) 파트 헤더(키워드 기반)
        if s0 and PART_LIKE_PAT.search(s0):
            part_name = s0
            part_common = part_unreserv = part_prep = None
            in_phys_table = False
            continue

        if not part_name:
            # 파트가 아직 정해지지 않았으면 스킵
            continue

        # 4) 파트 내부 섹션
        if s1:
            if "공통사항" in s1:
                part_common = norm_txt(row[2]); in_phys_table = False; continue
            if "진료불가" in s1:
                part_unreserv = norm_txt(row[2]); in_phys_table = False; continue
            if "준비사항" in s1:
                part_prep = norm_txt(row[2]); in_phys_table = False; continue
            if "주치의" in s1:
                in_phys_table = True
                phys_headers = [norm_txt(h) for h in row[1:] if pd.notna(h)]
                continue

        # 5) 의사 테이블 본문
        if in_phys_table:
            if pd.isna(row[1]):
                in_phys_table = False
                continue

            # 헤더 길이에 맞춰 슬라이싱
            values = list(row[1:1+len(phys_headers)])
            data: Dict[str, Any] = {}
            for h, v in zip(phys_headers, values):
                eng = HEADER_MAP.get(h or "", h or "")
                eng = re.sub(r"[^a-zA-Z0-9_]", "_", eng)
                data[eng] = norm_txt(v)

            # ---- 필드 정규화/추가 ----
            physician_name = data.get("physician_name")
            gender, grad_year, birth_year = parse_gender_year(physician_name or "")
            availability_start = availability_end = None
            # 의사명 줄에 '25.8~25.12' 같은 가용기간이 붙는 경우 파싱
            m_rng = re.search(r"\b\d{2}\.\d{1,2}\s*~\s*\d{2}\.\d{1,2}\b", physician_name or "")
            if m_rng:
                availability_start, availability_end = parse_year_month_range(m_rng.group(0))

            # 리스트형
            specialty_list = split_list(data.get("specialty"))
            unreserv_list = split_list(data.get("unreservable_conditions"))

            # 불리언 후보들
            guardian_bool, guardian_notes = parse_bool_with_notes(data.get("guardian_consultation"))
            foreign_bool, foreign_notes = parse_bool_with_notes(data.get("foreign_patient_care"))
            indepth_bool, indepth_notes = parse_bool_with_notes(data.get("in_depth_treatment"))
            carbon_bool, carbon_notes = parse_bool_with_notes(data.get("carbon_ion_therapy"))

            # 응급 슬롯 유무만 빨리 필터할 수 있게 플래그
            emergency_raw = data.get("emergency_slots")
            has_emergency = None
            if emergency_raw:
                has_emergency = False if emergency_raw.strip().lower() in {"x"} else True

            # 검색 합본(하이브리드 대비)
            content_fields = [
                data.get("notes"),
                part_common, part_unreserv, part_prep, dept_rules,
                data.get("fast_track_disease"),
                data.get("in_depth_treatment"),
                data.get("emergency_slots")
            ]
            searchable_content_ko = "\n\n".join([t for t in content_fields if t]) or None

            doc = {
                "id": make_id(dept_name, part_name, physician_name),
                "department_name": dept_name,
                "department_code": extract_dept_code(dept_name),
                "department_rules": dept_rules,
                "part_name": part_name,
                "common_rules": part_common,
                "unreservable_rules": part_unreserv,
                "preparation": part_prep,

                "physician_name": physician_name,
                "gender": gender,
                "graduation_year": grad_year,
                "birth_year": birth_year,
                "availability_start": availability_start,
                "availability_end": availability_end,

                "specialty": specialty_list,
                "notes": data.get("notes"),
                "unreservable_conditions": unreserv_list,

                "emergency_slots": emergency_raw,
                "has_emergency_slots": has_emergency,

                "in_depth_treatment": data.get("in_depth_treatment"),
                "has_in_depth_treatment": indepth_bool,
                "in_depth_treatment_notes": indepth_notes,

                "fast_track_disease": data.get("fast_track_disease"),

                "carbon_ion_therapy": data.get("carbon_ion_therapy"),
                "has_carbon_ion_therapy": carbon_bool,
                "carbon_ion_therapy_notes": carbon_notes,

                "guardian_consultation": data.get("guardian_consultation"),
                "guardian_consultation_bool": guardian_bool,
                "guardian_consultation_notes": guardian_notes,

                "foreign_patient_care": data.get("foreign_patient_care"),
                "foreign_patient_care_bool": foreign_bool,
                "foreign_patient_care_notes": foreign_notes,

                "searchable_content_ko": searchable_content_ko,

                # 추적성(선택)
                "source_file": str(filepath),
                "row_index": int(idx),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            flat.append(doc)

    return flat

def convert_nan_to_none(data):
    if isinstance(data, dict):
        return {k: convert_nan_to_none(v) for k, v in data.items()}
    if isinstance(data, list):
        return [convert_nan_to_none(x) for x in data]
    try:
        if pd.isna(data):
            return None
    except Exception:
        pass
    return data

# ---------- 실행 ----------
def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    parsed = parse_hospital_guidelines_flat(INPUT_XLSX)
    parsed = convert_nan_to_none(parsed)

    # JSON 배열
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)

    # JSONL (문서당 1행) - Azure Search 푸시/인덱서에 편리
    with open(OUT_JSONL, "w", encoding="utf-8") as f:
        for doc in parsed:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print(f"데이터가 {OUT_JSON} / {OUT_JSONL} 에 저장되었습니다.")

if __name__ == "__main__":
    main()
    print("=== preprocess.py 실행 종료 ===")
