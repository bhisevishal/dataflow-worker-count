# Dataflow worker count using golang client.

## Go Cloud Client Libraries:

*   https://cloud.google.com/apis/docs/cloud-client-libraries

*   https://cloud.google.com/go/docs/reference

*   https://cloud.google.com/go/docs/reference/cloud.google.com/go/dataflow/latest/apiv1beta3

## golang dev environment:

  Please check https://cloud.google.com/go/docs/setup for more details on how set
  up golang dev environment.

  Or you can use cloud shell:

  *   Open google cloud shell - https://shell.cloud.google.com
  *   Documentation: http://cloud/shell/docs/using-cloud-shell

Note: Please read shell scripts and code before running.

## Cleanup previous dataflow_worker_count client:

```
./cleanup.sh
```

## Build dataflow_worker_count client:

```
./build.sh
```

## To print help:

```
./dataflow_worker_count --help;
```

Note: Ensure you are authenticated or update `dataflow_worker_count.go` and use
appropriate credential option mentioned in
https://pkg.go.dev/google.golang.org/api/option.

## Example command to print desired worker count:

```
# Please make sure to set required environment variables or direct use values.
./dataflow_worker_count \
  --project_id="{PROJECT_ID:?}" \
  --location="{REGION:?}" \
  --job_id="{JOB_ID:?}" \
  --verbose=false \
;
```
