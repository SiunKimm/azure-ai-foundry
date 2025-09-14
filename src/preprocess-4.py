print("=== preprocess_scope_split.py 시작 ===")
import pandas as pd
import json
import re
from pathlib import Path
from datetime import datetime

SRC = "data/Azure_DataSet.xlsx"
OUT_DIR = Path("output")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# -----------------------------
# 유틸
# -----------------------------
def dept_code_from_name(name: str) -> str:
    if not name: return None
    m = re.match(r"^\s*([A-Z]{2,})\s*\.", str(name))
    return m.group(1) if m else None

def norm_text(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip()
    return s if s else None

def bool_from_free_text(x, default=False):
    s = norm_text(x)
    if s is None:
        return default
    low = s.lower()
    if any(k in low for k in ["불가", "절대불가", "금지"]): return False
    if low == "x": return False
    if "가능" in s: return True
    return True

def has_value_and_not_x(x):
    s = norm_text(x)
    if s is None: return False
    return s.lower() != "x"

def to_list(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    if isinstance(x, list): return x
    items = [t.strip() for t in re.split(r"[,\n]", str(x)) if t.strip()]
    return items or None

def convert_nan(obj):
    if isinstance(obj, dict):
        return {k: convert_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_nan(v) for v in obj]
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    return obj

# -----------------------------
# 원본 파서
# -----------------------------
def parse_flat(filepath):
    df = pd.read_excel(filepath, header=None)

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

    flat = []
    curr_dept = None
    curr_dept_rules = None
    curr_part = None
    part_common = None
    part_unres = None
    part_prep = None

    in_phys_tbl = False
    phy_headers = []

    for _, row in df.iterrows():
        if pd.notna(row[0]) and str(row[0]).strip().endswith('과'):
            curr_dept = str(row[0]).strip()
            curr_dept_rules = None
            curr_part = None
            part_common = None
            part_unres = None
            part_prep = None
            in_phys_tbl = False
            continue

        if curr_dept is None:
            continue

        if pd.notna(row[0]) and '진료과 공통사항' in str(row[0]):
            curr_dept_rules = row[2]
            continue

        if pd.notna(row[0]):
            curr_part = ' '.join(str(row[0]).split())
            part_common = None
            part_unres = None
            part_prep = None
            in_phys_tbl = False

        if curr_part is None:
            continue

        if pd.notna(row[1]):
            if '공통사항' in str(row[1]):
                part_common = row[2]
                in_phys_tbl = False
            elif '진료불가' in str(row[1]):
                part_unres = row[2]
                in_phys_tbl = False
            elif '준비사항' in str(row[1]):
                part_prep = row[2]
                in_phys_tbl = False
            elif '주치의' in str(row[1]):
                in_phys_tbl = True
                phy_headers = [h.strip() for h in row[1:] if pd.notna(h)]
                continue

        if in_phys_tbl and pd.notna(row[1]):
            data = {}
            row_vals = row[1:1+len(phy_headers)]
            for h, v in zip(phy_headers, row_vals):
                en = header_map.get(h, h)
                en = re.sub(r'[^a-zA-Z0-9_]', '_', en)
                data[en] = v

            rec = {
                "department_name": curr_dept,
                "department_rules": curr_dept_rules,
                "part_name": curr_part,
                "common_rules": part_common,
                "unreservable_rules": part_unres,
                "preparation": part_prep
            }
            rec.update(data)
            flat.append(rec)
        elif in_phys_tbl and pd.isna(row[1]):
            in_phys_tbl = False

    return flat

# -----------------------------
# 스코프 분리 + Boolean 필드 보강
# -----------------------------
def build_docs(flat):
    now_iso = datetime.utcnow().isoformat() + "Z"

    # 공통 Boolean 필드 초기화
    default_booleans = {
        "has_emergency_slots": None,
        "has_in_depth_treatment": None,
        "has_carbon_ion_therapy": None,
        "guardian_consultation_bool": None,
        "foreign_patient_care_bool": None
    }

    dept_rules_map = {}
    part_rules_map = {}

    for r in flat:
        dn = norm_text(r.get("department_name"))
        pn = norm_text(r.get("part_name"))
        if dn and r.get("department_rules") is not None:
            dept_rules_map[dn] = r.get("department_rules")
        if dn and pn:
            part_rules_map[(dn, pn)] = (
                r.get("common_rules"),
                r.get("unreservable_rules"),
                r.get("preparation")
            )

    docs = []

    # 부서 문서
    for dn, dept_rules in dept_rules_map.items():
        doc = {
            "id": f"{dn}|_dept",
            "department_code": dept_code_from_name(dn),
            "department_name": dn,
            "doc_scope": "department_rules",
            "department_rules": norm_text(dept_rules),
            "searchable_content_ko": norm_text(dept_rules),
            "ingested_at": now_iso,
        }
        doc.update(default_booleans)
        docs.append(convert_nan(doc))

    # 파트 문서
    for (dn, pn), (c, u, p) in part_rules_map.items():
        parts = [("공통사항", c), ("진료불가", u), ("준비사항", p)]
        body = "\n".join([f"*{t}*\n{norm_text(v)}" for t, v in parts if norm_text(v)])
        if not body:
            continue
        doc = {
            "id": f"{dn}|{pn}|_part",
            "department_code": dept_code_from_name(dn),
            "department_name": dn,
            "part_name": pn,
            "doc_scope": "part_rules",
            "common_rules": norm_text(c),
            "unreservable_rules": norm_text(u),
            "preparation": norm_text(p),
            "searchable_content_ko": body,
            "ingested_at": now_iso,
        }
        doc.update(default_booleans)
        docs.append(convert_nan(doc))

    # 의사 문서
    for r in flat:
        dn = norm_text(r.get("department_name"))
        pn = norm_text(r.get("part_name"))
        phy = norm_text(r.get("physician_name"))
        if not (dn and pn and phy):
            continue

        guardian_bool = bool_from_free_text(r.get("guardian_consultation"), default=None)
        foreign_bool = bool_from_free_text(r.get("foreign_patient_care"), default=None)
        has_emerg = has_value_and_not_x(r.get("emergency_slots"))
        has_deep = has_value_and_not_x(r.get("in_depth_treatment"))
        has_carbon = has_value_and_not_x(r.get("carbon_ion_therapy"))

        grad_year = None
        m = re.search(r'(\d{2,4})\s*졸', str(r.get("physician_name") or ""))
        if m:
            y = m.group(1)
            grad_year = int(y) if len(y) == 4 else int("19"+y) if int(y) >= 50 else int("20"+y)

        specialty_list = to_list(r.get("specialty"))
        unres_list = to_list(r.get("unreservable_conditions"))

        body_chunks = [
            norm_text(r.get("notes")),
            norm_text(r.get("specialty")),
            norm_text(r.get("fast_track_disease")),
            norm_text(r.get("emergency_slots")),
            norm_text(r.get("in_depth_treatment")),
            norm_text(r.get("carbon_ion_therapy")),
            norm_text(r.get("unreservable_conditions")),
            norm_text(r.get("foreign_patient_care_notes")),
        ]
        body = "\n".join([c for c in body_chunks if c])

        doc = {
            "id": f"{dn}|{pn}|{phy}",
            "department_code": dept_code_from_name(dn),
            "department_name": dn,
            "part_name": pn,
            "doc_scope": "physician",
            "physician_name": phy,
            "gender": None,
            "graduation_year": grad_year,
            "specialty": specialty_list,
            "notes": norm_text(r.get("notes")),
            "unreservable_conditions": unres_list,
            "emergency_slots": norm_text(r.get("emergency_slots")),
            "has_emergency_slots": bool(has_emerg),
            "in_depth_treatment": norm_text(r.get("in_depth_treatment")),
            "has_in_depth_treatment": bool(has_deep),
            "fast_track_disease": norm_text(r.get("fast_track_disease")),
            "carbon_ion_therapy": norm_text(r.get("carbon_ion_therapy")),
            "has_carbon_ion_therapy": bool(has_carbon),
            "guardian_consultation": norm_text(r.get("guardian_consultation")),
            "guardian_consultation_bool": guardian_bool,
            "foreign_patient_care": norm_text(r.get("foreign_patient_care")),
            "foreign_patient_care_notes": norm_text(r.get("foreign_patient_care_notes")),
            "foreign_patient_care_bool": foreign_bool,
            "searchable_content_ko": body if body else None,
            "source_file": "Azure_DataSet.xlsx",
            "row_index": None,
            "ingested_at": now_iso,
        }
        doc.update(default_booleans)
        docs.append(convert_nan(doc))

    return docs

# -----------------------------
# 실행
# -----------------------------
flat = parse_flat(SRC)
docs = build_docs(flat)

with open(OUT_DIR / "preprocessed_scoped.json", "w", encoding="utf-8") as f:
    json.dump(docs, f, ensure_ascii=False, indent=2)

with open(OUT_DIR / "preprocessed_scoped.jsonl", "w", encoding="utf-8") as f:
    for d in docs:
        f.write(json.dumps(d, ensure_ascii=False) + "\n")

print(f"생성 문서 수: {len(docs)}")
print("=== preprocess_scope_split.py 종료 ===")
