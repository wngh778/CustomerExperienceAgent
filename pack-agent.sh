#!/usr/bin/env bash
# 현재 작업트리의 저장소 코드만 ZIP으로 압축하고 반입용 .xlsx 확장자로 저장한다.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_NAME="$(basename "$SCRIPT_DIR")"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
DEFAULT_OUTPUT="$SCRIPT_DIR/${PROJECT_NAME}_${TIMESTAMP}.xlsx"
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
git rev-parse --is-inside-work-tree >/dev/null

FILE_LIST="$(mktemp "${TMPDIR:-/tmp}/tele-fraud-pack.XXXXXX")"
trap 'rm -f "$FILE_LIST"' EXIT

# 추적 파일 + 아직 커밋하지 않은 비무시 파일을 포함한다.
# .env, 실데이터, 캐시, 피드백, *.xlsx 등은 .gitignore 규칙에 따라 제외된다.
git ls-files -z --cached --others --exclude-standard >"$FILE_LIST"

python3 - "$SCRIPT_DIR" "$OUTPUT_PATH" "$FILE_LIST" <<'PY'
import os
import sys
import tempfile
import zipfile
from pathlib import Path


root = Path(sys.argv[1]).resolve()
output = Path(sys.argv[2]).resolve()
file_list = Path(sys.argv[3])

raw_paths = file_list.read_bytes().split(b"\0")
relative_paths = [Path(os.fsdecode(raw)) for raw in raw_paths if raw]
files = []
for relative in relative_paths:
    source = root / relative
    if source.is_symlink() or not source.is_file():
        continue
    resolved = source.resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        continue
    if resolved == output:
        continue
    files.append((resolved, relative.as_posix()))

if not files:
    raise SystemExit("압축할 저장소 파일이 없습니다.")

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

size_mb = output.stat().st_size / (1024 * 1024)
print(f"생성 완료: {output}")
print(f"포함 파일: {len(files)}개 / 크기: {size_mb:.2f} MB")
print("주의: 실제 형식은 ZIP이며 내부망 반입을 위해 확장자만 .xlsx로 저장했습니다.")
PY
