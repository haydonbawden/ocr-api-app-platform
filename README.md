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


## Job tracking and retry behavior

This bundle now supports optional job tracking via `job_id`:

- send `job_id` as a query parameter, or `X-Job-ID` as a request header
- the app records job status in `/tmp/ocr-api/job_registry.json`
- duplicate requests with the same `job_id` will not loop forever

Behavior:
- first request for a `job_id`: processes normally
- if that attempt fails, one more attempt is allowed for the same `job_id`
- if the second attempt fails, later requests for the same `job_id` return an error payload instead of reprocessing
- if a job is already processing, the app returns a 409 error
- if a job already succeeded, the app returns a 409 error

Successful OCR responses include:
- `X-Job-ID`
- `X-OCR-Attempt`


### Update: duplicate handling relaxed

- Duplicate or repeated requests with the same `job_id` are now allowed
- The app no longer blocks:
  - in-progress jobs
  - previously successful jobs
- Only restriction remaining:
  - a job that has failed twice will not be retried again
