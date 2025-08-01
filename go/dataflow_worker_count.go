// Package main is a tool to get the latest desired worker count for a dataflow job.
// External example - Get Dataflow worker count using golang client.
//
// Example usage:
//
//	go run dataflow_worker_count.go --project_id="my-project" --location="us-central1" --job_id="my-job" --time_delta_minutes=0 --min_worker=1 --max_worker=1000 --fetch_job_status=true --verbose=true;
package main

import (
	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	dataflowpb "cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	"context"
	"flag"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"os"
	"time"
)

func main() {
	projectID := flag.String("project_id", "", "Your Google Cloud project ID. (required)")
	location := flag.String("location", "", "The regional endpoint where the job is running (e.g., 'us-central1'). (required)")
	jobID := flag.String("job_id", "", "The ID of the Dataflow job. (required)")
	timeDeltaMinutes := flag.Int("time_delta_minutes", 0, "Optional: The duration in minutes to look back for events. Defaults to 0 minutes.")
	credentialsPath := flag.String("credentials_path", "", "Optional: Path to your service account JSON key file. If not provided, default application credentials will be used.")
	minWorker := flag.Int64("min_worker", 0, "Optional: Minimum number of workers to cap the desired workers.")
	maxWorker := flag.Int64("max_worker", 0, "Optional: Maximum number of workers to cap the desired workers.")
	fetchJobStatus := flag.Bool("fetch_job_status", false, "Optional: Fetch the job's current status.")
	checkTargetWorkers := flag.Bool("check_target_workers", true, "Optional: Whether to consider target workers when determining desired workers, useful if the upscale event has not been actuated yet. Defaults to true.")
	verbose := flag.Bool("verbose", true, "Optional: If false, only prints the desired worker count. Defaults to true for detailed output.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprint(os.Stderr, "Retrieves the latest Dataflow job worker counts within a specified time window.\n\n")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "\nPrerequisites:")
		fmt.Fprintln(os.Stderr, "  - Authentication: Ensure you are authenticated.")
		fmt.Fprintln(os.Stderr, "    e.g., 'gcloud auth application-default login' or set GOOGLE_APPLICATION_CREDENTIALS.")
	}
	flag.Parse()

	if *projectID == "" || *location == "" || *jobID == "" {
		log.Println("Error: --project_id, --location, and --job_id are required.")
		flag.Usage()
		os.Exit(1)
	}
	if *minWorker > 0 && *maxWorker > 0 && *minWorker > *maxWorker {
		log.Fatalf("--min_worker (%d) cannot be greater than --max_worker (%d).", *minWorker, *maxWorker)
	}
	if *minWorker < 0 {
		log.Fatalf("--min_worker (%d) cannot be negative.", *minWorker)
	}
	if *maxWorker < 0 {
		log.Fatalf("--max_worker (%d) cannot be negative.", *maxWorker)
	}
	if *timeDeltaMinutes < 0 {
		log.Fatalf("--time_delta_minutes (%d) cannot be negative.", *timeDeltaMinutes)
	}

	ctx := context.Background()
	var opts []option.ClientOption
	if *credentialsPath != "" {
		opts = append(opts, option.WithCredentialsFile(*credentialsPath))
	}

	jobsClient, err := dataflow.NewJobsV1Beta3Client(ctx, opts...)
	if err != nil {
		log.Fatalf("Failed to create Dataflow Jobs client: %v", err)
	}
	defer jobsClient.Close()

	messagesClient, err := dataflow.NewMessagesV1Beta3Client(ctx, opts...)
	if err != nil {
		log.Fatalf("Failed to create Dataflow Messages client: %v", err)
	}
	defer messagesClient.Close()

	jobStatus := "N/A"
	if *fetchJobStatus {
		if *verbose {
			fmt.Println("Fetching job status...")
		}
		req := &dataflowpb.GetJobRequest{
			ProjectId: *projectID,
			Location:  *location,
			JobId:     *jobID,
		}
		job, err := jobsClient.GetJob(ctx, req)
		if err != nil {
			log.Fatalf("API Error fetching job details: %v", err)
		}
		jobStatus = dataflowpb.JobState_name[int32(job.GetCurrentState())]
	}

	st := time.Now().UTC().Add(-time.Duration(*timeDeltaMinutes) * time.Minute)
	startTime := timestamppb.New(st)

	if *verbose {
		fmt.Printf(
			"Fetching worker counts for job '%s' in project '%s' at location '%s', looking back %d minute(s)...\n",
			*jobID, *projectID, *location, *timeDeltaMinutes,
		)
	}

	var latestCurrentWorkerEvent, latestTargetWorkerEvent *dataflowpb.AutoscalingEvent
	var latestCurrentWorkerEventTime, latestTargetWorkerEventTime time.Time

	req := &dataflowpb.ListJobMessagesRequest{
		ProjectId:         *projectID,
		Location:          *location,
		JobId:             *jobID,
		MinimumImportance: dataflowpb.JobMessageImportance_JOB_MESSAGE_BASIC,
		StartTime:         startTime,
	}

	it := messagesClient.ListJobMessages(ctx, req)

	var lastResponse any
	for {
		// We call Next() to advance the page.
		// The individual JobMessage is not used here; we process events from the response page.
		_, err := it.Next()
		if err != nil && err != iterator.Done {
			log.Fatalf("API Error fetching job messages: %v", err)
		}

		// The iterator's Response field holds the raw response for the current page.
		if it.Response != nil && it.Response != lastResponse {
			lastResponse = it.Response
			resp, ok := it.Response.(*dataflowpb.ListJobMessagesResponse)
			if !ok {
				log.Printf("WARN: could not cast response to *dataflowpb.ListJobMessagesResponse")
				break // Exit loop if response type is unexpected
			}

			for _, event := range resp.AutoscalingEvents {
				eventTime := event.GetTime().AsTime()
				if event.GetCurrentNumWorkers() > 0 && (latestCurrentWorkerEvent == nil || eventTime.After(latestCurrentWorkerEventTime)) {
					latestCurrentWorkerEvent = event
					latestCurrentWorkerEventTime = eventTime
				}
				if *checkTargetWorkers && event.GetTargetNumWorkers() > 0 && (latestTargetWorkerEvent == nil || eventTime.After(latestTargetWorkerEventTime)) {
					latestTargetWorkerEvent = event
					latestTargetWorkerEventTime = eventTime
				}
			}
		}

		if err == iterator.Done {
			break
		}
	} // end of for loop

	var latestCurrentWorkers, latestTargetWorkers, latestDesiredWorkers int64 = 0, 0, 0
	if latestCurrentWorkerEvent == nil && latestTargetWorkerEvent == nil {
		log.Fatalf("No autoscaling events with current or target worker counts found in the last %d minute(s).\n", *timeDeltaMinutes)
	}

	if latestCurrentWorkerEvent != nil {
		latestCurrentWorkers = latestCurrentWorkerEvent.GetCurrentNumWorkers()
	}

	if *checkTargetWorkers && latestTargetWorkerEvent != nil {
		latestTargetWorkers = latestTargetWorkerEvent.GetTargetNumWorkers()
	}

	// `desiredWorkers` is the maximum of the latest current and target worker counts,
	// clamped by the optional --min_worker and --max_worker flags.
	var desiredWorkers int64
	hasDesired := false
	if latestCurrentWorkerEvent != nil {
		desiredWorkers = latestCurrentWorkers
		if latestTargetWorkerEvent != nil && latestTargetWorkers > desiredWorkers {
			desiredWorkers = latestTargetWorkers
		}
		hasDesired = true
	} else if latestTargetWorkerEvent != nil {
		desiredWorkers = latestTargetWorkers
		hasDesired = true
	}

	if hasDesired {
		if *minWorker > 0 && desiredWorkers < int64(*minWorker) {
			desiredWorkers = int64(*minWorker)
		}
		if *maxWorker > 0 && desiredWorkers > int64(*maxWorker) {
			desiredWorkers = int64(*maxWorker)
		}
		latestDesiredWorkers = desiredWorkers
	}

	if !*verbose {
		if hasDesired {
			fmt.Println(latestDesiredWorkers)
			return
		}
		log.Fatalf("Could not determine desired worker count. No autoscaling events with current or target worker counts found in the last %d minute(s).", *timeDeltaMinutes)
	}

	fmt.Println("\n--- Results ---")
	if *fetchJobStatus {
		fmt.Printf("Job Status: %s\n", jobStatus)
	}

	fmt.Printf("Latest Current Workers: %v\n", latestCurrentWorkers)
	if *checkTargetWorkers {
		fmt.Printf("Latest Target Workers: %v\n", latestTargetWorkers)
	}
	fmt.Printf("Min Workers: %d\n", *minWorker)
	fmt.Printf("Max Workers: %d\n", *maxWorker)
	fmt.Printf("Latest Desired Workers: %v\n", latestDesiredWorkers)
	fmt.Println("----------------")
}
