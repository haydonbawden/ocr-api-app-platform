import hashlib
import json
import io
import logging
import os
import shutil
import subprocess
import tempfile
import re
import time
import uuid
import fitz
from pathlib import Path
from PIL import Image
from typing import List, Optional

from fastapi import BackgroundTasks, Depends, FastAPI, File, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse

APP_NAME = "ocr-api"
TMP_DIR = os.getenv("TMP_DIR", "/tmp/ocr-api")
API_KEY = os.getenv("API_KEY", "change-me")
MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "50"))
OCR_TIMEOUT_SECONDS = int(os.getenv("OCR_TIMEOUT_SECONDS", "900"))
REGISTRY_PATH = os.path.join(TMP_DIR, "job_registry.json")

Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(APP_NAME)

app = FastAPI(title="OCR API", version="1.0.0")


def load_registry() -> dict:
    if not os.path.exists(REGISTRY_PATH):
        return {}
    try:
        with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.exception("job_registry_load_failed")
        return {}


def save_registry(registry: dict) -> None:
    tmp_path = REGISTRY_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(registry, f)
    os.replace(tmp_path, REGISTRY_PATH)


def get_tracking_job_id(job_id_query: Optional[str], x_job_id: Optional[str]) -> str:
    candidate = (job_id_query or x_job_id or "").strip()
    return candidate if candidate else str(uuid.uuid4())


def update_job_registry(job_id: str, status: str, attempts: int, last_error: Optional[str] = None) -> None:
    registry = load_registry()
    registry[job_id] = {"status": status, "attempts": attempts, "last_error": last_error}
    save_registry(registry)


def begin_job(job_id_query: Optional[str], x_job_id: Optional[str]) -> tuple[str, int]:
    tracking_job_id = get_tracking_job_id(job_id_query, x_job_id)
    registry = load_registry()
    existing = registry.get(tracking_job_id)
    if existing:
        attempts = int(existing.get("attempts", 0))
        if existing.get("status") == "failed" and attempts >= 2:
            raise HTTPException(
                status_code=422,
                detail={
                    "message": "Job failed twice and will not be reattempted",
                    "job_id": tracking_job_id,
                    "attempts": attempts,
                    "last_error": existing.get("last_error", "Unknown error"),
                },
            )
    attempts = int(existing.get("attempts", 0)) + 1 if existing else 1
    update_job_registry(tracking_job_id, "processing", attempts, None)
    return tracking_job_id, attempts


def verify_api_key(x_api_key: Optional[str] = Header(default=None)) -> None:
    if not x_api_key or x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.middleware("http")
async def add_request_logging(request: Request, call_next):
    start = time.time()
    request_id = str(uuid.uuid4())
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("unhandled_exception request_id=%s path=%s", request_id, request.url.path)
        raise
    duration_ms = int((time.time() - start) * 1000)
    response.headers["X-Request-ID"] = request_id
    logger.info(
        "request_complete request_id=%s method=%s path=%s status=%s duration_ms=%s",
        request_id, request.method, request.url.path, response.status_code, duration_ms,
    )
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    return {
        "name": APP_NAME,
        "status": "ok",
        "endpoints": {"health": "/health", "ocr": "/ocr", "merge": "/merge", "merge_ocr": "/merge-ocr", "page_count": "/page-count", "merge_autorotate": "/merge-autorotate"},
    }


def sha256_of_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def ensure_pdf_header(path: str) -> None:
    with open(path, "rb") as f:
        header = f.read(5)
    if header != b"%PDF-":
        raise HTTPException(status_code=400, detail="Uploaded file is not a valid PDF")


def save_upload_to_disk(upload: UploadFile, dest_path: str) -> int:
    total_bytes = 0
    max_bytes = MAX_UPLOAD_MB * 1024 * 1024
    with open(dest_path, "wb") as out_file:
        while True:
            chunk = upload.file.read(1024 * 1024)
            if not chunk:
                break
            total_bytes += len(chunk)
            if total_bytes > max_bytes:
                out_file.close()
                try:
                    os.remove(dest_path)
                except FileNotFoundError:
                    pass
                raise HTTPException(status_code=413, detail=f"File too large. Max allowed size is {MAX_UPLOAD_MB} MB")
            out_file.write(chunk)
    if total_bytes == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    return total_bytes


def build_ocr_command(input_path: str, output_path: str, language: str, force_ocr: bool, deskew: bool, rotate_pages: bool, optimize: int) -> list[str]:
    cmd = [
        "ocrmypdf", "--language", language, "--jobs", "1", "--optimize", str(optimize),
        "--output-type", "pdf", "--sidecar", "/dev/null",
    ]
    if force_ocr:
        cmd.append("--force-ocr")
    else:
        cmd.append("--skip-text")
    if deskew:
        cmd.append("--deskew")
    if rotate_pages:
        cmd.append("--rotate-pages")
    cmd.extend([input_path, output_path])
    return cmd



def get_pdf_page_count(path: str) -> int:
    result = subprocess.run(
        ["qpdf", "--show-npages", path],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "PDF page count failed")
    try:
        return int((result.stdout or "").strip())
    except ValueError as e:
        raise RuntimeError("Invalid page count output from qpdf") from e


def merge_pdfs_qpdf(input_paths: list[str], output_path: str) -> None:
    cmd = ["qpdf", "--empty", "--pages", *input_paths, "--", output_path]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "PDF merge failed")


def normalize_uploaded_files(files: Optional[List[UploadFile]], file: Optional[UploadFile]) -> List[UploadFile]:
    if files and len(files) > 0:
        return files
    if file is not None:
        return [file]
    raise HTTPException(status_code=400, detail="At least one PDF file is required")


async def extract_uploaded_files_from_request(request: Request) -> List[UploadFile]:
    form = await request.form()
    uploads: List[UploadFile] = []

    preferred_keys = ("file", "files", "files[]")
    for key in preferred_keys:
        value = form.getlist(key)
        for item in value:
            if hasattr(item, "filename"):
                uploads.append(item)

    if uploads:
        return uploads

    # Fallback: accept any multipart file fields, including names like files[0], files[1], etc.
    for key, item in form.multi_items():
        if hasattr(item, "filename"):
            uploads.append(item)

    if uploads:
        return uploads

    raise HTTPException(status_code=400, detail="At least one PDF file is required")


def get_request_value(request: Request, form, query_name: str, default_value: str) -> str:
    query_value = request.query_params.get(query_name)
    if query_value is not None and query_value != "":
        return query_value
    form_value = form.get(query_name)
    if form_value is not None and str(form_value) != "":
        return str(form_value)
    return default_value


def parse_bool_value(value, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def build_pdf_response(path: str, filename: str, background_tasks: BackgroundTasks, headers: dict[str, str]) -> FileResponse:
    response = FileResponse(path=path, media_type="application/pdf", filename=filename, background=background_tasks)
    for k, v in headers.items():
        response.headers[k] = v
    return response




def detect_page_rotation_degrees(page: fitz.Page) -> int:
    pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5), alpha=False)
    img = Image.open(io.BytesIO(pix.tobytes("png")))
    tmp_dir = tempfile.mkdtemp(prefix="osd-", dir=TMP_DIR)
    img_path = os.path.join(tmp_dir, "page.png")
    try:
        img.save(img_path, format="PNG")
        result = subprocess.run(
            ["tesseract", img_path, "stdout", "--psm", "0"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        osd_text = (result.stdout or "") + "\n" + (result.stderr or "")
        match = re.search(r"Rotate:\s*(\d+)", osd_text)
        if match:
            degrees = int(match.group(1)) % 360
            if degrees in (0, 90, 180, 270):
                return degrees
        return 0
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def autorotate_pdf(input_path: str, output_path: str) -> list[int]:
    src = fitz.open(input_path)
    applied: list[int] = []
    try:
        for page in src:
            detected = detect_page_rotation_degrees(page)
            if detected:
                page.set_rotation((page.rotation - detected) % 360)
            applied.append(detected)
        src.save(output_path)
    finally:
        src.close()
    return applied


def normalize_single_pdf_for_autorotate(input_path: str, output_path: str) -> tuple[bool, str]:
    """
    Try to rewrite a single uploaded PDF through qpdf to normalize malformed object tables.
    Returns (used_fallback, warning_text).
    """
    cmd = ["qpdf", input_path, output_path]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    stderr = (result.stderr or "").strip()
    stdout = (result.stdout or "").strip()
    warning_text = "\n".join([s for s in [stderr, stdout] if s]).strip()

    if result.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
        return True, warning_text

    # qpdf sometimes emits warnings and still writes an output file that is usable
    if os.path.exists(output_path) and os.path.getsize(output_path) > 0 and "operation succeeded with warnings" in warning_text:
        return True, warning_text

    # Not recoverable; leave caller to fall back to original or raise.
    if os.path.exists(output_path):
        try:
            os.remove(output_path)
        except Exception:
            pass
    return False, warning_text



@app.post("/page-count", dependencies=[Depends(verify_api_key)])
async def page_count_pdf_files(
    request: Request,
    job_id: Optional[str] = Query(default=None),
    x_job_id: Optional[str] = Header(default=None),
):
    tracking_job_id = get_tracking_job_id(job_id, x_job_id)
    request_id = str(uuid.uuid4())
    work_dir = tempfile.mkdtemp(prefix="page-count-", dir=TMP_DIR)
    uploads = []
    try:
        uploads = await extract_uploaded_files_from_request(request)
        total_pages = 0
        total_bytes = 0
        file_results = []

        logger.info(
            "page_count_start request_id=%s job_id=%s file_count=%s",
            request_id,
            tracking_job_id,
            len(uploads),
        )

        for idx, upload in enumerate(uploads, start=1):
            if upload.content_type not in ("application/pdf", "application/octet-stream"):
                raise HTTPException(status_code=400, detail="Only PDF uploads are supported")
            file_path = os.path.join(work_dir, f"input-{idx}.pdf")
            file_bytes = save_upload_to_disk(upload, file_path)
            ensure_pdf_header(file_path)
            pages = get_pdf_page_count(file_path)
            total_pages += pages
            total_bytes += file_bytes
            file_results.append(
                {
                    "filename": upload.filename or f"input-{idx}.pdf",
                    "pages": pages,
                    "bytes": file_bytes,
                }
            )

        logger.info(
            "page_count_success request_id=%s job_id=%s file_count=%s total_pages=%s total_bytes=%s",
            request_id,
            tracking_job_id,
            len(file_results),
            total_pages,
            total_bytes,
        )

        return {
            "job_id": tracking_job_id,
            "file_count": len(file_results),
            "total_pages": total_pages,
            "total_bytes": total_bytes,
            "files": file_results,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("page_count_unhandled_exception request_id=%s job_id=%s", request_id, tracking_job_id)
        raise HTTPException(
            status_code=500,
            detail={
                "message": "PDF page count failed",
                "job_id": tracking_job_id,
                "error": str(e)[:1000],
            },
        )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
        for upload in uploads:
            try:
                upload.file.close()
            except Exception:
                pass


@app.post("/merge-autorotate", dependencies=[Depends(verify_api_key)])
async def merge_autorotate_pdf_files(
    request: Request,
    background_tasks: BackgroundTasks,
    job_id: Optional[str] = Query(default=None),
    x_job_id: Optional[str] = Header(default=None),
):
    tracking_job_id, attempts = begin_job(job_id, x_job_id)
    request_id = str(uuid.uuid4())
    work_dir = tempfile.mkdtemp(prefix="merge-autorotate-", dir=TMP_DIR)
    merged_path = os.path.join(work_dir, "merged.pdf")
    rotated_path = os.path.join(work_dir, "merged-upright.pdf")
    uploads = []
    try:
        uploads = await extract_uploaded_files_from_request(request)
        total_bytes = 0
        saved_paths = []
        for idx, upload in enumerate(uploads, start=1):
            if upload.content_type not in ("application/pdf", "application/octet-stream"):
                raise HTTPException(status_code=400, detail="Only PDF uploads are supported")
            file_path = os.path.join(work_dir, f"input-{idx}.pdf")
            total_bytes += save_upload_to_disk(upload, file_path)
            ensure_pdf_header(file_path)
            saved_paths.append(file_path)

        logger.info(
            "merge_autorotate_start request_id=%s job_id=%s attempt=%s file_count=%s total_bytes=%s",
            request_id, tracking_job_id, attempts, len(saved_paths), total_bytes
        )

        merge_started = time.time()
        merge_warning = ""
        if len(saved_paths) == 1:
            repaired_single_path = os.path.join(work_dir, "single-normalized.pdf")
            used_fallback, merge_warning = normalize_single_pdf_for_autorotate(saved_paths[0], repaired_single_path)
            if used_fallback:
                shutil.copyfile(repaired_single_path, merged_path)
            else:
                shutil.copyfile(saved_paths[0], merged_path)
        else:
            merge_pdfs_qpdf(saved_paths, merged_path)
        merge_elapsed_ms = int((time.time() - merge_started) * 1000)

        rotate_started = time.time()
        applied_rotations = autorotate_pdf(merged_path, rotated_path)
        rotate_elapsed_ms = int((time.time() - rotate_started) * 1000)

        if not os.path.exists(rotated_path) or os.path.getsize(rotated_path) == 0:
            update_job_registry(tracking_job_id, "failed", attempts, "Autorotated output was not created")
            raise HTTPException(
                status_code=500,
                detail={"message": "Autorotated output was not created", "job_id": tracking_job_id, "attempts": attempts},
            )

        rotated_pages = sum(1 for d in applied_rotations if d)
        update_job_registry(tracking_job_id, "success", attempts, None)
        logger.info(
            "merge_autorotate_success request_id=%s job_id=%s attempt=%s merge_elapsed_ms=%s rotate_elapsed_ms=%s rotated_pages=%s output_bytes=%s",
            request_id, tracking_job_id, attempts, merge_elapsed_ms, rotate_elapsed_ms, rotated_pages, os.path.getsize(rotated_path)
        )
        if merge_warning:
            logger.warning(
                "merge_autorotate_single_file_qpdf_warning request_id=%s job_id=%s attempt=%s detail=%s",
                request_id, tracking_job_id, attempts, merge_warning[:2000]
            )

        def cleanup():
            shutil.rmtree(work_dir, ignore_errors=True)

        background_tasks.add_task(cleanup)
        return build_pdf_response(
            rotated_path,
            "merged-upright.pdf",
            background_tasks,
            {
                "X-Job-ID": tracking_job_id,
                "X-Merge-Autorotate-Attempt": str(attempts),
                "X-Rotated-Pages": str(rotated_pages),
            },
        )
    except subprocess.TimeoutExpired:
        shutil.rmtree(work_dir, ignore_errors=True)
        update_job_registry(tracking_job_id, "failed", attempts, "Merge+autorotate processing timed out")
        logger.error("merge_autorotate_timeout request_id=%s job_id=%s attempt=%s", request_id, tracking_job_id, attempts)
        raise HTTPException(
            status_code=504,
            detail={"message": "Merge+autorotate processing timed out", "job_id": tracking_job_id, "attempts": attempts},
        )
    except HTTPException:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise
    except Exception as e:
        shutil.rmtree(work_dir, ignore_errors=True)
        update_job_registry(tracking_job_id, "failed", attempts, str(e)[:1000] or "Merge+autorotate failed")
        logger.exception("merge_autorotate_unhandled_exception request_id=%s job_id=%s attempt=%s", request_id, tracking_job_id, attempts)
        raise HTTPException(
            status_code=500,
            detail={"message": "Merge+autorotate internal server error", "job_id": tracking_job_id, "attempts": attempts},
        )
    finally:
        for upload in uploads:
            try:
                upload.file.close()
            except Exception:
                pass


@app.post("/merge", dependencies=[Depends(verify_api_key)])

async def merge_pdf_files(
    background_tasks: BackgroundTasks,
    files: Optional[List[UploadFile]] = File(default=None),
    file: Optional[UploadFile] = File(default=None),
    job_id: Optional[str] = Query(default=None),
    x_job_id: Optional[str] = Header(default=None),
):
    tracking_job_id, attempts = begin_job(job_id, x_job_id)
    request_id = str(uuid.uuid4())
    work_dir = tempfile.mkdtemp(prefix="merge-", dir=TMP_DIR)
    output_path = os.path.join(work_dir, "merged.pdf")
    uploads = []
    try:
        uploads = await extract_uploaded_files_from_request(request)
        total_bytes = 0
        saved_paths = []
        for idx, upload in enumerate(uploads, start=1):
            if upload.content_type not in ("application/pdf", "application/octet-stream"):
                raise HTTPException(status_code=400, detail="Only PDF uploads are supported")
            file_path = os.path.join(work_dir, f"input-{idx}.pdf")
            total_bytes += save_upload_to_disk(upload, file_path)
            ensure_pdf_header(file_path)
            saved_paths.append(file_path)
        logger.info("merge_start request_id=%s job_id=%s attempt=%s file_count=%s total_bytes=%s", request_id, tracking_job_id, attempts, len(saved_paths), total_bytes)
        started = time.time()
        merge_pdfs_qpdf(saved_paths, output_path)
        elapsed_ms = int((time.time() - started) * 1000)
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            update_job_registry(tracking_job_id, "failed", attempts, "Merged output was not created")
            raise HTTPException(status_code=500, detail={"message": "Merged output was not created", "job_id": tracking_job_id, "attempts": attempts})
        update_job_registry(tracking_job_id, "success", attempts, None)
        logger.info("merge_success request_id=%s job_id=%s attempt=%s elapsed_ms=%s output_bytes=%s", request_id, tracking_job_id, attempts, elapsed_ms, os.path.getsize(output_path))
        def cleanup():
            shutil.rmtree(work_dir, ignore_errors=True)
        background_tasks.add_task(cleanup)
        return build_pdf_response(output_path, "merged.pdf", background_tasks, {"X-Job-ID": tracking_job_id, "X-Merge-Attempt": str(attempts)})
    except HTTPException:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise
    except Exception as e:
        shutil.rmtree(work_dir, ignore_errors=True)
        update_job_registry(tracking_job_id, "failed", attempts, str(e)[:1000] or "Merge failed")
        logger.exception("merge_unhandled_exception request_id=%s job_id=%s attempt=%s", request_id, tracking_job_id, attempts)
        raise HTTPException(status_code=500, detail={"message": "PDF merge failed", "job_id": tracking_job_id, "attempts": attempts})
    finally:
        for upload in uploads:
            try:
                upload.file.close()
            except Exception:
                pass


@app.post("/merge-ocr", dependencies=[Depends(verify_api_key)])
async def merge_and_ocr_pdf_files(
    request: Request,
    background_tasks: BackgroundTasks,
    job_id: Optional[str] = Query(default=None),
    x_job_id: Optional[str] = Header(default=None),
):
    form = await request.form()
    language = get_request_value(request, form, "language", "eng")
    force_ocr = parse_bool_value(request.query_params.get("force_ocr", form.get("force_ocr")), False)
    deskew = parse_bool_value(request.query_params.get("deskew", form.get("deskew")), True)
    rotate_pages = parse_bool_value(request.query_params.get("rotate_pages", form.get("rotate_pages")), True)
    optimize_raw = request.query_params.get("optimize", form.get("optimize"))
    optimize = int(optimize_raw) if optimize_raw not in (None, "") else 1
    if optimize not in (0, 1, 2, 3):
        raise HTTPException(status_code=400, detail="optimize must be one of: 0, 1, 2, 3")
    tracking_job_id, attempts = begin_job(job_id, x_job_id)
    request_id = str(uuid.uuid4())
    work_dir = tempfile.mkdtemp(prefix="merge-ocr-", dir=TMP_DIR)
    merged_path = os.path.join(work_dir, "merged.pdf")
    output_path = os.path.join(work_dir, "merged-searchable.pdf")
    uploads = []
    try:
        uploads = await extract_uploaded_files_from_request(request)
        total_bytes = 0
        saved_paths = []
        for idx, upload in enumerate(uploads, start=1):
            if upload.content_type not in ("application/pdf", "application/octet-stream"):
                raise HTTPException(status_code=400, detail="Only PDF uploads are supported")
            file_path = os.path.join(work_dir, f"input-{idx}.pdf")
            total_bytes += save_upload_to_disk(upload, file_path)
            ensure_pdf_header(file_path)
            saved_paths.append(file_path)
        logger.info("merge_ocr_start request_id=%s job_id=%s attempt=%s file_count=%s total_bytes=%s language=%s force_ocr=%s", request_id, tracking_job_id, attempts, len(saved_paths), total_bytes, language, force_ocr)
        merge_started = time.time()
        merge_pdfs_qpdf(saved_paths, merged_path)
        merge_elapsed_ms = int((time.time() - merge_started) * 1000)
        input_sha = sha256_of_file(merged_path)
        cmd = build_ocr_command(merged_path, output_path, language, force_ocr, deskew, rotate_pages, optimize)
        logger.info("ocr_start request_id=%s job_id=%s attempt=%s filename=%s bytes=%s sha256=%s language=%s force_ocr=%s", request_id, tracking_job_id, attempts, "merged.pdf", os.path.getsize(merged_path), input_sha, language, force_ocr)
        started = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=OCR_TIMEOUT_SECONDS, check=False)
        elapsed_ms = int((time.time() - started) * 1000)
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            update_job_registry(tracking_job_id, "failed", attempts, stderr[-1000:] or stdout[-1000:] or "Merge+OCR processing failed")
            logger.error("merge_ocr_failed request_id=%s job_id=%s attempt=%s merge_elapsed_ms=%s ocr_elapsed_ms=%s returncode=%s stdout=%r stderr=%r", request_id, tracking_job_id, attempts, merge_elapsed_ms, elapsed_ms, result.returncode, stdout[-2000:], stderr[-2000:])
            raise HTTPException(status_code=500, detail={"message": "Merge+OCR processing failed", "job_id": tracking_job_id, "attempts": attempts})
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            update_job_registry(tracking_job_id, "failed", attempts, "Merge+OCR output was not created")
            logger.error("merge_ocr_missing_output request_id=%s job_id=%s attempt=%s", request_id, tracking_job_id, attempts)
            raise HTTPException(status_code=500, detail={"message": "Merge+OCR output was not created", "job_id": tracking_job_id, "attempts": attempts})
        update_job_registry(tracking_job_id, "success", attempts, None)
        logger.info("merge_ocr_success request_id=%s job_id=%s attempt=%s merge_elapsed_ms=%s ocr_elapsed_ms=%s output_bytes=%s", request_id, tracking_job_id, attempts, merge_elapsed_ms, elapsed_ms, os.path.getsize(output_path))
        def cleanup():
            shutil.rmtree(work_dir, ignore_errors=True)
        background_tasks.add_task(cleanup)
        return build_pdf_response(output_path, "merged-searchable.pdf", background_tasks, {"X-Job-ID": tracking_job_id, "X-Merge-OCR-Attempt": str(attempts)})
    except subprocess.TimeoutExpired:
        shutil.rmtree(work_dir, ignore_errors=True)
        update_job_registry(tracking_job_id, "failed", attempts, f"Merge+OCR processing timed out after {OCR_TIMEOUT_SECONDS} seconds")
        logger.error("merge_ocr_timeout request_id=%s job_id=%s attempt=%s timeout_seconds=%s", request_id, tracking_job_id, attempts, OCR_TIMEOUT_SECONDS)
        raise HTTPException(status_code=504, detail={"message": "Merge+OCR processing timed out", "job_id": tracking_job_id, "attempts": attempts})
    except HTTPException:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise
    except Exception as e:
        shutil.rmtree(work_dir, ignore_errors=True)
        update_job_registry(tracking_job_id, "failed", attempts, str(e)[:1000] or "Merge+OCR failed")
        logger.exception("merge_ocr_unhandled_exception request_id=%s job_id=%s attempt=%s", request_id, tracking_job_id, attempts)
        raise HTTPException(status_code=500, detail={"message": "Merge+OCR internal server error", "job_id": tracking_job_id, "attempts": attempts})
    finally:
        for upload in uploads:
            try:
                upload.file.close()
            except Exception:
                pass


@app.post("/ocr", dependencies=[Depends(verify_api_key)])
async def ocr_pdf(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    language: str = "eng",
    force_ocr: bool = False,
    deskew: bool = True,
    rotate_pages: bool = True,
    optimize: int = 1,
    job_id: Optional[str] = Query(default=None),
    x_job_id: Optional[str] = Header(default=None),
):
    if file.content_type not in ("application/pdf", "application/octet-stream"):
        raise HTTPException(status_code=400, detail="Only PDF uploads are supported")
    if optimize not in (0, 1, 2, 3):
        raise HTTPException(status_code=400, detail="optimize must be one of: 0, 1, 2, 3")
    tracking_job_id, attempts = begin_job(job_id, x_job_id)
    request_id = str(uuid.uuid4())
    work_dir = tempfile.mkdtemp(prefix="job-", dir=TMP_DIR)
    input_path = os.path.join(work_dir, "input.pdf")
    output_path = os.path.join(work_dir, "output-searchable.pdf")
    try:
        bytes_written = save_upload_to_disk(file, input_path)
        ensure_pdf_header(input_path)
        input_sha = sha256_of_file(input_path)
        cmd = build_ocr_command(input_path, output_path, language, force_ocr, deskew, rotate_pages, optimize)
        logger.info("ocr_start request_id=%s job_id=%s attempt=%s filename=%s bytes=%s sha256=%s language=%s force_ocr=%s", request_id, tracking_job_id, attempts, file.filename, bytes_written, input_sha, language, force_ocr)
        started = time.time()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=OCR_TIMEOUT_SECONDS, check=False)
        elapsed_ms = int((time.time() - started) * 1000)
        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            update_job_registry(tracking_job_id, "failed", attempts, stderr[-1000:] or stdout[-1000:] or "OCR processing failed")
            logger.error("ocr_failed request_id=%s job_id=%s attempt=%s returncode=%s elapsed_ms=%s stdout=%r stderr=%r", request_id, tracking_job_id, attempts, result.returncode, elapsed_ms, stdout[-2000:], stderr[-2000:])
            raise HTTPException(status_code=500, detail={"message": "OCR processing failed", "job_id": tracking_job_id, "attempts": attempts})
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            update_job_registry(tracking_job_id, "failed", attempts, "OCR output was not created")
            logger.error("ocr_missing_output request_id=%s job_id=%s attempt=%s elapsed_ms=%s", request_id, tracking_job_id, attempts, elapsed_ms)
            raise HTTPException(status_code=500, detail={"message": "OCR output was not created", "job_id": tracking_job_id, "attempts": attempts})
        update_job_registry(tracking_job_id, "success", attempts, None)
        logger.info("ocr_success request_id=%s job_id=%s attempt=%s elapsed_ms=%s output_bytes=%s", request_id, tracking_job_id, attempts, elapsed_ms, os.path.getsize(output_path))
        def cleanup():
            shutil.rmtree(work_dir, ignore_errors=True)
        background_tasks.add_task(cleanup)
        safe_name = "searchable-" + (file.filename or "document.pdf")
        if not safe_name.lower().endswith(".pdf"):
            safe_name += ".pdf"
        return build_pdf_response(output_path, safe_name, background_tasks, {"X-Job-ID": tracking_job_id, "X-OCR-Attempt": str(attempts)})
    except subprocess.TimeoutExpired:
        shutil.rmtree(work_dir, ignore_errors=True)
        update_job_registry(tracking_job_id, "failed", attempts, f"OCR processing timed out after {OCR_TIMEOUT_SECONDS} seconds")
        logger.error("ocr_timeout request_id=%s job_id=%s attempt=%s timeout_seconds=%s", request_id, tracking_job_id, attempts, OCR_TIMEOUT_SECONDS)
        raise HTTPException(status_code=504, detail={"message": "OCR processing timed out", "job_id": tracking_job_id, "attempts": attempts})
    except HTTPException:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise
    except Exception:
        shutil.rmtree(work_dir, ignore_errors=True)
        update_job_registry(tracking_job_id, "failed", attempts, "Internal server error")
        logger.exception("ocr_unhandled_exception request_id=%s job_id=%s attempt=%s", request_id, tracking_job_id, attempts)
        raise HTTPException(status_code=500, detail={"message": "Internal server error", "job_id": tracking_job_id, "attempts": attempts})
    finally:
        try:
            file.file.close()
        except Exception:
            pass
