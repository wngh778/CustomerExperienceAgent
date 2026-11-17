import os
import sys; sys.path.append(".")
from core.pii_masking import (
    check_pii, 
    mask_pii, 
    mask_md_bytes,
    mask_docx_bytes,
    mask_xlsx_bytes
)

# cd app && python tests/pii_test.py

for dir_path, _, filenames in os.walk("output/report"):
    for fname in filenames:
        # if fname != "pii_test.md": continue
        print(f"==================================")
        print(f"파일 이름 : {fname}")
        
        path = os.path.join(dir_path, fname)
    
        with open(path, 'rb') as f:
            binary_data = f.read()
    
        if fname.endswith(".md"):
            res, check_pii = mask_md_bytes(binary_data)
        elif fname.endswith(".docx"):
            res, check_pii = mask_docx_bytes(binary_data)
        elif fname.endswith(".xlsx"):
            res, check_pii = mask_xlsx_bytes(binary_data)
    
        pii_res = "검출" if check_pii else "미검출"
    
        print(f"개인정보 검출결과 : {pii_res}")
        