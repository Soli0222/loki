package query

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/gorilla/websocket"
	"github.com/grafana/dskit/backoff"

	"github.com/grafana/loki/v3/pkg/logcli/client"
	"github.com/grafana/loki/v3/pkg/logcli/output"
	"github.com/grafana/loki/v3/pkg/logcli/util"
	"github.com/grafana/loki/v3/pkg/loghttp"
	"github.com/grafana/loki/v3/pkg/util/unmarshal"
)

// TailQuery connects to the Loki websocket endpoint and tails logs
func (q *Query) TailQuery(delayFor time.Duration, c client.Client, out output.LogOutput) error {
	// Backward-compatible wrapper without cancellation.
	return q.TailQueryWithContext(context.Background(), delayFor, c, out)
}

// TailQueryWithContext tails logs and supports graceful cancellation via ctx.
func (q *Query) TailQueryWithContext(ctx context.Context, delayFor time.Duration, c client.Client, out output.LogOutput) error {
	conn, err := c.LiveTailQueryConn(q.QueryString, delayFor, q.Limit, q.Start, q.Quiet)
	if err != nil {
		return fmt.Errorf("tailing logs failed: %w", err)
	}

	// Close the connection when context is canceled.
	go func() {
		<-ctx.Done()
		// Attempt graceful close; ignore errors as we're already stopping.
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		_ = conn.Close()
	}()

	if len(q.IgnoreLabelsKey) > 0 && !q.Quiet {
		log.Println("Ignoring labels key:", color.RedString(strings.Join(q.IgnoreLabelsKey, ",")))
	}

	if len(q.ShowLabelsKey) > 0 && !q.Quiet {
		log.Println("Print only labels key:", color.RedString(strings.Join(q.ShowLabelsKey, ",")))
	}

	lastReceivedTimestamp := q.Start

	for {
		tailResponse := new(loghttp.TailResponse)
		err := unmarshal.ReadTailResponseJSON(tailResponse, conn)
		if err != nil {
			// If context was canceled, treat as graceful termination.
			if ctx.Err() != nil {
				return nil
			}

			// Check if the websocket connection closed unexpectedly. If so, retry.
			// The connection might close unexpectedly if the querier handling the tail request
			// in Loki stops running. The following error would be printed:
			// "websocket: close 1006 (abnormal closure): unexpected EOF"
			if websocket.IsCloseError(err, websocket.CloseAbnormalClosure) {
				log.Printf("Remote websocket connection closed unexpectedly (%+v). Connecting again.", err)

				// Ensure previous broken connection is closed.
				_ = conn.Close()

				// Try to re-establish the connection up to 5 times.
				backoff := backoff.New(context.Background(), backoff.Config{
					MinBackoff: 1 * time.Second,
					MaxBackoff: 10 * time.Second,
					MaxRetries: 5,
				})

				for backoff.Ongoing() {
					// If ctx is canceled while backing off, stop.
					if ctx.Err() != nil {
						return nil
					}
					conn, err = c.LiveTailQueryConn(q.QueryString, delayFor, q.Limit, lastReceivedTimestamp, q.Quiet)
					if err == nil {
						break
					}

					log.Println("Error recreating tailing connection after unexpected close, will retry:", err)
					backoff.Wait()
				}

				if err = backoff.Err(); err != nil {
					log.Println("Error recreating tailing connection:", err)
					return fmt.Errorf("error recreating tailing connection: %w", err)
				}

				continue
			}

			// Treat normal closure as graceful termination
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return nil
			}

			log.Println("Error reading stream:", err)
			return fmt.Errorf("error reading stream: %w", err)
		}

		labels := loghttp.LabelSet{}
		for _, stream := range tailResponse.Streams {
			if !q.NoLabels {
				if len(q.IgnoreLabelsKey) > 0 || len(q.ShowLabelsKey) > 0 {

					ls := stream.Labels

					if len(q.ShowLabelsKey) > 0 {
						ls = matchLabels(true, ls, q.ShowLabelsKey)
					}

					if len(q.IgnoreLabelsKey) > 0 {
						ls = matchLabels(false, ls, q.ShowLabelsKey)
					}

					labels = ls

				} else {
					labels = stream.Labels
				}
			}

			for _, entry := range stream.Entries {
				out.FormatAndPrintln(entry.Timestamp, labels, 0, entry.Line)
				lastReceivedTimestamp = entry.Timestamp
			}

		}
		if len(tailResponse.DroppedStreams) != 0 {
			log.Println("Server dropped following entries due to slow client")
			for _, d := range tailResponse.DroppedStreams {
				log.Println(d.Timestamp, d.Labels)
			}
		}
	}
}

func matchLabels(on bool, l loghttp.LabelSet, names []string) loghttp.LabelSet {
	return util.MatchLabels(on, l, names)
}
