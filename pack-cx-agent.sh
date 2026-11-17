#!/usr/bin/env bash
# CX Agent 실행에 필요한 핵심 코드만 ZIP으로 압축하고 내부망 반입용 .xlsx 확장자로 저장한다.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
DEFAULT_OUTPUT="$SCRIPT_DIR/cx-agent_${TIMESTAMP}.xlsx"
OUTPUT_PATH="${1:-$DEFAULT_OUTPUT}"

if [[ "$OUTPUT_PATH" != /* ]]; then
  OUTPUT_PATH="$PWD/$OUTPUT_PATH"
fi
if [[ "$OUTPUT_PATH" != *.xlsx ]]; then
  OUTPUT_PATH="${OUTPUT_PATH}.xlsx"
fi
if [[ -e "$OUTPUT_PATH" ]]; then
  echo "이미 존재하는 파일입니다: $OUTPUT_PATH" >&2
  exit 1
fi

cd "$SCRIPT_DIR"

python3 - "$SCRIPT_DIR" "$OUTPUT_PATH" <<'PY'
import os
import sys
import tempfile
import zipfile
from pathlib import Path

root = Path(sys.argv[1]).resolve()
output = Path(sys.argv[2]).resolve()

# 내부망 반입 대상 경로 (실행에 필요한 핵심 코드 및 빌드 설정만 포함)
INCLUDE_TARGETS = [
    "Dockerfile",
    "requirements.txt",
    "app/agent",
    "app/api",
    "app/core",
    "app/tests",
]

# 제외할 디렉토리명 / 파일명 / 확장자
EXCLUDE_DIRS = {
    "__pycache__",
    ".ipynb_checkpoints",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".git",
    ".venv",
    "logs",
    "output",
    "evaluation",
}
EXCLUDE_FILES = {
    ".DS_Store",
    ".env",
    "mcp_auth_token.json",
}
EXCLUDE_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".lock",
    ".tmp",
    ".log",
    ".db",
    ".xlsx",
    ".zip",
    ".docx",
    ".sh",
}


def should_exclude(rel_path: Path) -> bool:
    for part in rel_path.parts[:-1]:
        if part in EXCLUDE_DIRS or part.startswith(".venv"):
            return True
    name = rel_path.name
    if name in EXCLUDE_FILES or name.startswith(".env") or name.endswith("-checkpoint.py"):
        return True
    if rel_path.suffix.lower() in EXCLUDE_SUFFIXES:
        return True
    return False


files: list[tuple[Path, str]] = []
for target in INCLUDE_TARGETS:
    target_path = root / target
    if not target_path.exists() or target_path.is_symlink():
        continue
    candidates = [target_path] if target_path.is_file() else sorted(target_path.rglob("*"))
    for candidate in candidates:
        if candidate.is_symlink() or not candidate.is_file():
            continue
        resolved = candidate.resolve()
        try:
            rel = resolved.relative_to(root)
        except ValueError:
            continue
        if resolved == output or should_exclude(rel):
            continue
        files.append((resolved, rel.as_posix()))

if not files:
    raise SystemExit("압축할 코드 파일이 없습니다.")

output.parent.mkdir(parents=True, exist_ok=True)
fd, temporary_name = tempfile.mkstemp(
    prefix=f".{output.name}.",
    suffix=".tmp",
    dir=str(output.parent),
)
os.close(fd)
temporary = Path(temporary_name)
try:
    with zipfile.ZipFile(
        temporary,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for source, archive_name in sorted(files, key=lambda item: item[1]):
            archive.write(source, archive_name)
    temporary.replace(output)
except BaseException:
    temporary.unlink(missing_ok=True)
    raise

size_kb = output.stat().st_size / 1024
print(f"생성 완료: {output}")
print(f"포함 파일: {len(files)}개 / 크기: {size_kb:.1f} KB")
print("주의: 실제 형식은 ZIP이며 내부망 반입을 위해 확장자만 .xlsx로 저장했습니다.")
PY
