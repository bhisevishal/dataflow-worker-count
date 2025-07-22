#!/bin/bash


# --- Function to display usage information ---
usage() {
  echo "Usage: $0 --project-id <PROJECT_ID> --location <LOCATION> --job-id <JOB_ID> [--time-delta-minutes <MINUTES>] [--credentials-path <PATH>] [--min-worker <COUNT>] [--max-worker <COUNT>]"
  echo ""
  echo "Retrieve latest Dataflow job worker counts within a specified time window."
  echo ""
  echo "Arguments:"
  echo "  --project-id <PROJECT_ID>         Your Google Cloud project ID (required)."
  echo "  --location <LOCATION>             The regional endpoint where the job is running (e.g., 'us-central1', 'europe-west1') (required)."
  echo "  --job-id <JOB_ID>                 The ID of the Dataflow job (required)."
  echo "  --time-delta-minutes <MINUTES>    Optional: The duration in minutes to look back for events. Defaults to 10 minutes."
  echo "  --credentials-path <PATH>         Optional: Path to your service account JSON key file. If not provided,"
  echo "                                    default application credentials will be used (e.g., from GOOGLE_APPLICATION_CREDENTIALS)."
  echo "  --min-worker <COUNT>              Optional: Minimum number of workers to cap the desired workers."
  echo "  --max-worker <COUNT>              Optional: Maximum number of workers to cap the desired workers."
  echo "  --no-fetch-status                 Optional: Disable fetching the job's current status."
  echo ""
  echo "Prerequisites:"
  echo "  - curl: For making HTTP requests."
  echo "  - jq: For parsing JSON responses."
  echo "  - gcloud CLI: For authentication. Ensure you are authenticated (e.g., 'gcloud auth application-default login' or service account activation)."
  exit 1
}

# --- Function to check for missing flag arguments ---
check_arg() {
    local flag="$1"
    local value="$2"
    if [[ -z "$value" ]]; then
        echo "Error: Missing argument for $flag" >&2
        usage
    fi
    if [[ "$value" == --* ]]; then
        echo "Error: Value '${value}' for $flag is not a valid argument value." >&2
        usage
    fi
}

# --- Function to validate non-negative integer arguments ---
check_non_negative_integer() {
    local flag="$1"
    local value="$2"
    if ! [[ "$value" =~ ^[0-9]+$ ]]; then
        echo "Error: ${flag} must be a non-negative integer, but got '${value}'." >&2
        usage
    fi
}

# FYI: Another hacky way to get worker count. This assume dataflow using GCE workers.
# WORKER_COUNT=$(gcloud compute instances list --filter="labels.dataflow_job_id:${JOB_ID:?}" --format="get(NAME)" | wc -l);

# --- Check for prerequisites ---
command -v curl >/dev/null 2>&1 || { echo >&2 "Error: 'curl' is not installed. Please install it."; exit 1; }
command -v jq >/dev/null 2>&1 || { echo >&2 "Error: 'jq' is not installed. Please install it (e.g., 'sudo apt-get install jq' or 'brew install jq')."; exit 1; }
command -v gcloud >/dev/null 2>&1 || { echo >&2 "Error: 'gcloud' CLI is not installed. Authentication requires gcloud. Please install it."; exit 1; }

# --- Initialize variables with default values ---
PROJECT_ID=""
LOCATION=""
JOB_ID=""
TIME_DELTA_MINUTES=10
CREDENTIALS_PATH=""
MIN_WORKER=0
MAX_WORKER=0
FETCH_JOB_STATUS=true

# --- Parse command-line arguments ---
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --project-id=*)
            PROJECT_ID="${1#*=}"
            ;;
        --project-id)
            check_arg "$1" "$2"
            PROJECT_ID="$2"
            shift
            ;;
        --location=*)
            LOCATION="${1#*=}"
            ;;
        --location)
            check_arg "$1" "$2"
            LOCATION="$2"
            shift
            ;;
        --job-id=*)
            JOB_ID="${1#*=}"
            ;;
        --job-id)
            check_arg "$1" "$2"
            JOB_ID="$2"
            shift
            ;;
        --time-delta-minutes=*)
            TIME_DELTA_MINUTES="${1#*=}"
            check_non_negative_integer "--time-delta-minutes" "$TIME_DELTA_MINUTES"
            ;;
        --time-delta-minutes)
            check_arg "$1" "$2"
            check_non_negative_integer "$1" "$2"
            TIME_DELTA_MINUTES="$2"
            shift
            ;;
        --credentials-path=*)
            CREDENTIALS_PATH="${1#*=}"
            ;;
        --credentials-path)
            check_arg "$1" "$2"
            CREDENTIALS_PATH="$2"
            shift
            ;;
        --min-worker=*)
            MIN_WORKER="${1#*=}"
            check_non_negative_integer "--min-worker" "$MIN_WORKER"
            ;;
        --min-worker)
            check_arg "$1" "$2"
            check_non_negative_integer "$1" "$2"
            MIN_WORKER="$2"
            shift
            ;;
        --max-worker=*)
            MAX_WORKER="${1#*=}"
            check_non_negative_integer "--max-worker" "$MAX_WORKER"
            ;;
        --max-worker)
            check_arg "$1" "$2"
            check_non_negative_integer "$1" "$2"
            MAX_WORKER="$2"
            shift
            ;;
        --no-fetch-status)
            FETCH_JOB_STATUS=false
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown parameter passed: $1"
            usage
            ;;
    esac
    shift
done

# --- Validate required arguments ---
if [ -z "$PROJECT_ID" ] || [ -z "$LOCATION" ] || [ -z "$JOB_ID" ]; then
    echo "Error: --project-id, --location, and --job-id are required."
    usage
fi

# --- Validate min-worker and max-worker ---
if [ "$MIN_WORKER" -gt 0 ] && [ "$MAX_WORKER" -gt 0 ] && [ "$MIN_WORKER" -gt "$MAX_WORKER" ]; then
    echo "Error: --min-worker ($MIN_WORKER) cannot be greater than --max-worker ($MAX_WORKER)."
    usage
fi

# --- Authenticate and get access token ---
ACCESS_TOKEN=""
if [ -n "$CREDENTIALS_PATH" ]; then
    if [ ! -f "$CREDENTIALS_PATH" ]; then
        echo "Error: Service account key file not found at '$CREDENTIALS_PATH'."
        echo "Please ensure the path is correct or use default authentication."
        exit 1
    fi
    # If a credentials path is given, try to activate the service account for gcloud
    # This assumes the user has given gcloud permissions to do so.
    # Note: A pure bash script doesn't directly parse the JSON key file for auth
    # without gcloud.
    echo "Attempting to activate service account from '$CREDENTIALS_PATH'..."
    gcloud auth activate-service-account --key-file="$CREDENTIALS_PATH" >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "Error: Failed to activate service account. Check key file or permissions."
        exit 1
    fi
fi

# Get the access token using gcloud application-default credentials
ACCESS_TOKEN=$(gcloud auth application-default print-access-token 2>/dev/null)
if [ -z "$ACCESS_TOKEN" ]; then
    echo "Error: Could not obtain Google Cloud access token."
    echo "Please ensure you are authenticated (e.g., 'gcloud auth application-default login') or provide a service account key via --credentials-path."
    exit 1
fi

JOB_STATUS="N/A"
# --- Get Job Status ---
if [ "$FETCH_JOB_STATUS" = true ]; then
    echo "Fetching job status..."
    JOB_DETAILS_URL="https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${LOCATION}/jobs/${JOB_ID}"
    JOB_DETAILS_RESPONSE=$(curl -s -H "Authorization: Bearer ${ACCESS_TOKEN}" "${JOB_DETAILS_URL}")

    # --- Error handling for curl call ---
    if [ $? -ne 0 ]; then
        echo "Error: 'curl' command failed to connect to Dataflow API for job details."
        exit 1
    fi

    # --- Check for API errors in the JSON response ---
    ERROR_MESSAGE=$(echo "$JOB_DETAILS_RESPONSE" | jq -r '.error.message // empty')
    if [ -n "$ERROR_MESSAGE" ]; then
        if echo "$JOB_DETAILS_RESPONSE" | jq -e '.error.status == "NOT_FOUND"' >/dev/null 2>&1; then
            echo "Error: Job with ID '${JOB_ID}' not found in project '${PROJECT_ID}' at location '${LOCATION}'."
            echo "API Error fetching job details: $ERROR_MESSAGE"
        else
            echo "API Error fetching job details: $ERROR_MESSAGE"
            echo "Full API Response: $JOB_DETAILS_RESPONSE" | jq . # Pretty print the error response
        fi
        exit 1
    fi
    JOB_STATUS=$(echo "$JOB_DETAILS_RESPONSE" | jq -r '.currentState // "N/A"')
fi

# --- Calculate startTime in RFC 3339 format ---
# Get current UTC time in seconds since epoch
NOW_UTC_SECONDS=$(date -u +%s)
# Calculate start time in seconds since epoch
START_TIME_UTC_SECONDS=$((NOW_UTC_SECONDS - TIME_DELTA_MINUTES * 60))
# Convert to RFC 3339 format (e.g., "2014-10-02T15:01:23Z")
# We're omitting fractional seconds for simplicity, which is allowed by RFC 3339.
START_TIME_RFC3339=$(date -u -d "@$START_TIME_UTC_SECONDS" +"%Y-%m-%dT%H:%M:%SZ")

# --- Construct the API URL ---
# See API documentation: https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs.messages/list
# API Explorer: https://developers.google.com/apis-explorer/#p/dataflow/v1b3/dataflow.projects.locations.jobs.messages.list
API_URL_BASE="https://dataflow.googleapis.com/v1b3/projects/${PROJECT_ID}/locations/${LOCATION}/jobs/${JOB_ID}/messages?startTime=${START_TIME_RFC3339}&minimumImportance=JOB_MESSAGE_BASIC&pageSize=100"

echo "Fetching worker counts for job '${JOB_ID}' in project '${PROJECT_ID}' at location '${LOCATION}', looking back ${TIME_DELTA_MINUTES} minute(s)..."

# --- Make the API call using curl and handle pagination ---
ALL_AUTOSCALING_EVENTS="[]"
PAGE_TOKEN=""

while true; do
    if [ -n "$PAGE_TOKEN" ]; then
        CURRENT_API_URL="${API_URL_BASE}&pageToken=${PAGE_TOKEN}"
    else
        CURRENT_API_URL="${API_URL_BASE}"
    fi

    RESPONSE=$(curl -s -H "Authorization: Bearer ${ACCESS_TOKEN}" "${CURRENT_API_URL}")

    # --- Error handling for curl call ---
    if [ $? -ne 0 ]; then
        echo "Error: 'curl' command failed to connect to Dataflow API."
        exit 1
    fi

    # --- Check for API errors in the JSON response ---
    ERROR_MESSAGE=$(echo "$RESPONSE" | jq -r '.error.message // empty')
    if [ -n "$ERROR_MESSAGE" ]; then
        if echo "$RESPONSE" | jq -e '.error.status == "NOT_FOUND"' >/dev/null 2>&1; then
            echo "Error: Job with ID '${JOB_ID}' not found in project '${PROJECT_ID}' at location '${LOCATION}'."
        else
            echo "API Error: $ERROR_MESSAGE"
            echo "Full API Response: $RESPONSE" | jq . # Pretty print the error response
        fi
        exit 1
    fi

    # Extract autoscaling events from the current page
    CURRENT_EVENTS=$(echo "$RESPONSE" | jq -c '.autoscalingEvents // []')

    # Merge current events with all events
    ALL_AUTOSCALING_EVENTS=$(jq -c '.[0] + .[1]' <<< "[$ALL_AUTOSCALING_EVENTS, $CURRENT_EVENTS]")

    # Get the next page token
    PAGE_TOKEN=$(echo "$RESPONSE" | jq -r '.nextPageToken // empty')

    # Exit loop if there's no next page token
    if [ -z "$PAGE_TOKEN" ]; then
        break
    fi
done

# --- Parse the JSON response using jq ---
LATEST_CURRENT_WORKERS="N/A"
LATEST_TARGET_WORKERS="N/A"
LATEST_DESIRED_WORKERS="N/A"

if [ "$(echo "$ALL_AUTOSCALING_EVENTS" | jq 'length')" -eq 0 ]; then
    echo "No autoscaling events found in the last ${TIME_DELTA_MINUTES} minute(s)."
else
    # Get the latest currentNumWorkers and targetNumWorkers from all events,
    # as they may appear in different event messages.
    # The 'time' field in autoscaling events is an RFC3339 timestamp string,
    # which is lexicographically sortable. This allows 'sort_by(.time)' to
    # correctly order events chronologically.
    LATEST_CURRENT_WORKERS=$(echo "$ALL_AUTOSCALING_EVENTS" | jq -r '[.[] | select(.currentNumWorkers != null)] | sort_by(.time) | .[-1].currentNumWorkers // "N/A"')
    LATEST_TARGET_WORKERS=$(echo "$ALL_AUTOSCALING_EVENTS" | jq -r '[.[] | select(.targetNumWorkers != null)] | sort_by(.time) | .[-1].targetNumWorkers // "N/A"')

    # Calculate the maximum of current and target workers
    if [[ "$LATEST_CURRENT_WORKERS" =~ ^[0-9]+$ ]] && [[ "$LATEST_TARGET_WORKERS" =~ ^[0-9]+$ ]]; then
        if [ "$LATEST_CURRENT_WORKERS" -gt "$LATEST_TARGET_WORKERS" ]; then
            LATEST_DESIRED_WORKERS="$LATEST_CURRENT_WORKERS"
        else
            LATEST_DESIRED_WORKERS="$LATEST_TARGET_WORKERS"
        fi

        # Cap desired workers within min/max range if provided
        if [ "$MIN_WORKER" -gt 0 ] && [ "$LATEST_DESIRED_WORKERS" -lt "$MIN_WORKER" ]; then
            LATEST_DESIRED_WORKERS="$MIN_WORKER"
        fi
        if [ "$MAX_WORKER" -gt 0 ] && [ "$LATEST_DESIRED_WORKERS" -gt "$MAX_WORKER" ]; then
            LATEST_DESIRED_WORKERS="$MAX_WORKER"
        fi
    else
        LATEST_DESIRED_WORKERS="N/A"
    fi
fi

echo "--- Results ---"
if [ "$FETCH_JOB_STATUS" = true ]; then
    echo "Job Status: ${JOB_STATUS}"
fi
echo "Latest Current Workers: ${LATEST_CURRENT_WORKERS}"
echo "Latest Target Workers: ${LATEST_TARGET_WORKERS}"
echo "Min Workers: ${MIN_WORKER}"
echo "Max Workers: ${MAX_WORKER}"
echo "Latest Desired Workers: ${LATEST_DESIRED_WORKERS}"
echo "----------------"
