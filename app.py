import hashlib
import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Optional

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
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
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
        "endpoints": {
            "health": "/health",
            "ocr": "/ocr",
        },
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
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large. Max allowed size is {MAX_UPLOAD_MB} MB",
                )
            out_file.write(chunk)

    if total_bytes == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    return total_bytes


def build_ocr_command(
    input_path: str,
    output_path: str,
    language: str,
    force_ocr: bool,
    deskew: bool,
    rotate_pages: bool,
    optimize: int,
) -> list[str]:
    cmd = [
        "ocrmypdf",
        "--language",
        language,
        "--jobs",
        "1",
        "--optimize",
        str(optimize),
        "--output-type",
        "pdf",
        "--sidecar",
        "/dev/null",
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

    tracking_job_id = get_tracking_job_id(job_id, x_job_id)
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
        if status == "failed" and attempts >= 2:
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
    registry[tracking_job_id] = {
        "status": "processing",
        "attempts": attempts,
        "last_error": None,
    }
    save_registry(registry)

    request_id = str(uuid.uuid4())
    work_dir = tempfile.mkdtemp(prefix="job-", dir=TMP_DIR)
    input_path = os.path.join(work_dir, "input.pdf")
    output_path = os.path.join(work_dir, "output-searchable.pdf")

    try:
        bytes_written = save_upload_to_disk(file, input_path)
        ensure_pdf_header(input_path)

        input_sha = sha256_of_file(input_path)

        cmd = build_ocr_command(
            input_path=input_path,
            output_path=output_path,
            language=language,
            force_ocr=force_ocr,
            deskew=deskew,
            rotate_pages=rotate_pages,
            optimize=optimize,
        )

        logger.info(
            "ocr_start request_id=%s job_id=%s attempt=%s filename=%s bytes=%s sha256=%s language=%s force_ocr=%s",
            request_id,
            tracking_job_id,
            attempts,
            file.filename,
            bytes_written,
            input_sha,
            language,
            force_ocr,
        )

        started = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=OCR_TIMEOUT_SECONDS,
            check=False,
        )
        elapsed_ms = int((time.time() - started) * 1000)

        if result.returncode != 0:
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            registry = load_registry()
            registry[tracking_job_id] = {
                "status": "failed",
                "attempts": attempts,
                "last_error": stderr[-1000:] or stdout[-1000:] or "OCR processing failed",
            }
            save_registry(registry)

            logger.error(
                "ocr_failed request_id=%s job_id=%s attempt=%s returncode=%s elapsed_ms=%s stdout=%r stderr=%r",
                request_id,
                tracking_job_id,
                attempts,
                result.returncode,
                elapsed_ms,
                stdout[-2000:],
                stderr[-2000:],
            )
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "OCR processing failed",
                    "job_id": tracking_job_id,
                    "attempts": attempts,
                },
            )

        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            registry = load_registry()
            registry[tracking_job_id] = {
                "status": "failed",
                "attempts": attempts,
                "last_error": "OCR output was not created",
            }
            save_registry(registry)
            logger.error("ocr_missing_output request_id=%s job_id=%s attempt=%s elapsed_ms=%s", request_id, tracking_job_id, attempts, elapsed_ms)
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "OCR output was not created",
                    "job_id": tracking_job_id,
                    "attempts": attempts,
                },
            )

        output_name = "searchable-" + (file.filename or "document.pdf")
        safe_output_name = output_name if output_name.lower().endswith(".pdf") else f"{output_name}.pdf"

        def cleanup():
            shutil.rmtree(work_dir, ignore_errors=True)

        background_tasks.add_task(cleanup)

        registry = load_registry()
        registry[tracking_job_id] = {
            "status": "success",
            "attempts": attempts,
            "last_error": None,
        }
        save_registry(registry)

        logger.info(
            "ocr_success request_id=%s job_id=%s attempt=%s elapsed_ms=%s output_bytes=%s",
            request_id,
            tracking_job_id,
            attempts,
            elapsed_ms,
            os.path.getsize(output_path),
        )

        response = FileResponse(
            path=output_path,
            media_type="application/pdf",
            filename=safe_output_name,
            background=background_tasks,
        )
        response.headers["X-Job-ID"] = tracking_job_id
        response.headers["X-OCR-Attempt"] = str(attempts)
        return response

    except subprocess.TimeoutExpired:
        shutil.rmtree(work_dir, ignore_errors=True)
        registry = load_registry()
        registry[tracking_job_id] = {
            "status": "failed",
            "attempts": attempts,
            "last_error": f"OCR processing timed out after {OCR_TIMEOUT_SECONDS} seconds",
        }
        save_registry(registry)
        logger.error("ocr_timeout request_id=%s job_id=%s attempt=%s timeout_seconds=%s", request_id, tracking_job_id, attempts, OCR_TIMEOUT_SECONDS)
        raise HTTPException(
            status_code=504,
            detail={
                "message": "OCR processing timed out",
                "job_id": tracking_job_id,
                "attempts": attempts,
            },
        )
    except HTTPException:
        shutil.rmtree(work_dir, ignore_errors=True)
        raise
    except Exception:
        shutil.rmtree(work_dir, ignore_errors=True)
        registry = load_registry()
        registry[tracking_job_id] = {
            "status": "failed",
            "attempts": attempts,
            "last_error": "Internal server error",
        }
        save_registry(registry)
        logger.exception("ocr_unhandled_exception request_id=%s job_id=%s attempt=%s", request_id, tracking_job_id, attempts)
        raise HTTPException(
            status_code=500,
            detail={
                "message": "Internal server error",
                "job_id": tracking_job_id,
                "attempts": attempts,
            },
        )
    finally:
        try:
            file.file.close()
        except Exception:
            pass
