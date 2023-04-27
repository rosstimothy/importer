package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
)

func main() {
	var traces = flag.String("dir", "", "Path to traces")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Kill, os.Interrupt)
	defer cancel()

	client := otlptracegrpc.NewClient(
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
		otlptracegrpc.WithInsecure(),
	)

	dialCtx, dialCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dialCancel()

	if err := client.Start(dialCtx); err != nil {
		log.Fatalf("failed to start client: %v", err)
	}
	defer client.Stop(context.Background())

	var spans []*tracepb.ResourceSpans
	if err := filepath.WalkDir(*traces, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return nil
		}
		defer f.Close()

		log.Printf("Exporting trace %q", path)

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024*1024)
		scanner.Split(bufio.ScanLines)

		for scanner.Scan() {
			var span tracepb.ResourceSpans
			if err := jsonpb.UnmarshalString(scanner.Text(), &span); err != nil {
				return nil
			}
			spans = append(spans, &span)

			if len(spans) == 50 {
				if err := client.UploadTraces(ctx, spans); err != nil {
					log.Fatalf("failed to upload traces: %v", err)
				}
				spans = nil
			}
		}
		if len(spans) > 0 {
			if err := client.UploadTraces(ctx, spans); err != nil {
				log.Fatalf("failed to upload traces: %v", err)
			}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("problem reading trace file: %w", err)
		}

		return nil
	}); err != nil {
		log.Fatalf("failed to parse traces in %q: %v", *traces, err)
	}

	if err := client.Stop(context.Background()); err != nil {
		log.Fatalf("failed to stop client: %v", err)
	}
}
