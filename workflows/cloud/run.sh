# config
export CLOUDSDK_CORE_PROJECT="project-id"
export CLOUDSDK_CORE_ACCOUNT="service_account"
export REGION="us-east4"
export SERVICE_NAME="service_name_api"
export IMAGE_NAME="gcr.io/${CLOUDSDK_CORE_PROJECT}/${SERVICE_NAME}"

# build image
gcloud builds submit --tag "${IMAGE_NAME}"

# deploy to cloud run
gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE_NAME}" \
  --region "${REGION}" \
  --platform managed \
  --service-account "${CLOUDSDK_CORE_ACCOUNT}" \
  --no-allow-unauthenticated

# service url
SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
  --region "${REGION}" \
  --format="value(status.url)")

GCLOUD_TOKEN=$(gcloud auth print-identity-token)

# test API
curl -k -X POST "${SERVICE_URL}/decompress" \
  -H "Authorization: Bearer ${GCLOUD_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
        "bucket_name": "bucket-name",
        "input_file": "file-name.zst",
        "output_file": "output-file.json"
      }'