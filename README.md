# OCR API

A Dockerized FastAPI service that accepts a PDF and returns a searchable PDF using OCRmyPDF.

## Features

- Searchable PDF output
- API key authentication
- Health endpoint
- Size limit and timeout controls
- Structured logs
- Docker-ready for DigitalOcean App Platform

## Endpoints

- `GET /health`
- `GET /`
- `POST /ocr`

## Required environment variables

- `API_KEY`
- `MAX_UPLOAD_MB` (default: `50`)
- `OCR_TIMEOUT_SECONDS` (default: `900`)
- `LOG_LEVEL` (default: `INFO`)
- `WEB_CONCURRENCY` (default: `2`)
- `GUNICORN_TIMEOUT` (default: `930`)

## Local build

```bash
docker build -t ocr-api .
docker run --rm -p 8080:8080 \
  -e API_KEY="super-secret-key" \
  -e MAX_UPLOAD_MB=100 \
  -e OCR_TIMEOUT_SECONDS=900 \
  ocr-api
```

## Test request

```bash
curl -X POST "http://localhost:8080/ocr?language=eng&deskew=true&rotate_pages=true&optimize=1" \
  -H "X-API-Key: super-secret-key" \
  -F "file=@input.pdf" \
  --output searchable.pdf
```

## Deploy to DigitalOcean App Platform

1. Push this repo to GitHub.
2. In DigitalOcean, create a new App from GitHub.
3. Select the repo and deploy using the included `.do/app.yaml`.
4. Set the `API_KEY` environment variable in App Platform before going live.

## Notes

- Local disk is ephemeral on App Platform. This app processes files in `/tmp` and streams the result back immediately.
- For larger workflows, the next upgrade is to store outputs in DigitalOcean Spaces and return a signed URL instead of the file itself.
- For non-English OCR, install extra `tesseract-ocr-<lang>` packages in the Dockerfile and pass `language=<lang>` to the endpoint.


## Resource tuning update

This bundle has been updated for App Platform OCR stability:

- OCRmyPDF now runs with `--jobs 1` to reduce peak memory usage
- `.do/app.yaml` now targets `apps-s-1vcpu-1gb`
- `WEB_CONCURRENCY` in `.do/app.yaml` is set to `1`

If you still see container exits on larger scanned PDFs, move to a 2 GB instance.


## Single-file and multi-file upload compatibility

`/merge` and `/merge-ocr` accept either:
- a single multipart file field named `file`
- multiple multipart file fields named `files`

The app normalizes both internally.

## Merge API endpoints

- `POST /merge`
- `POST /merge-ocr`

Both support `job_id` and `X-Job-ID`.


## Page count API endpoint

This bundle now also includes:

- `POST /page-count`
  - accepts either a single multipart `file` field or repeated multipart `files` fields
  - counts pages using `qpdf --show-npages`
  - returns JSON only


## Multipart compatibility update

`/merge`, `/merge-ocr`, and `/page-count` now accept a broader range of multipart upload shapes.

Supported field names include:
- `file`
- `files`
- `files[]`
- indexed names such as `files[0]`, `files[1]`

This is intended to make integrations from Make and Gravity Forms more tolerant of differing multipart encodings.


## Merge + autorotate endpoint

This bundle now also includes:

- `POST /merge-autorotate`
  - accepts one or many PDFs
  - merges them
  - detects page orientation with Tesseract OSD
  - rotates sideways pages upright
  - returns `merged-upright.pdf`

Recommended flow:
1. `POST /merge-autorotate`
2. `POST /ocr` with fast OCR settings (`deskew=false`, `rotate_pages=false`, `optimize=0`)

### Dependency note
This endpoint uses:
- PyMuPDF (`fitz`)
- Pillow (`PIL`)


## Autorotate dependency update

This bundle now explicitly installs:
- `PyMuPDF` for `fitz`
- `Pillow` for image processing

If a deployment previously failed with `ModuleNotFoundError: No module named 'fitz'`, this updated bundle addresses that by including the required Python dependencies in `requirements.txt`.


## Single-file malformed PDF tolerance

`POST /merge-autorotate` now tolerates qpdf warnings for malformed **single uploaded PDFs**.

Behavior:
- if one PDF is uploaded, the service first asks qpdf to normalize/rewrite that one file
- if qpdf reports warnings but still produces a usable output file, the workflow proceeds
- for multiple uploaded PDFs, the normal merge path is unchanged

This is intended to avoid rejecting single-file uploads solely because qpdf reports object-table warnings while still producing a usable PDF.
