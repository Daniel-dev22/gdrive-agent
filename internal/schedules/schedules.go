// Package schedules is the gdrive-side adapter for the shared per-agent
// scheduler in github.com/Daniel-dev22/agent-kit-go/scheduler.
//
// The shared kit owns the cron tick, the reconcile pull from
// controller, the on-disk schedules.json cache (atomic write), and
// missed-run recovery. This adapter supplies what is gdrive-specific:
//
//   - the FireFunc that maps a schedule's RepoID into a backup job via
//     the existing job.Runner.StartBackup pipeline.
//   - the constructor wiring the agent's pooled router HTTP client +
//     config.Config into a scheduler.Config.
//
// Per CLAUDE.md ("Agent Scheduling Lives In The Agent"), scheduler-api
// and EDA rulebooks are NOT in the firing path. The agent owns its own
// cron evaluation so a controller pod restart cannot miss a fire
// window.
package schedules

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/rs/zerolog/log"

	kitsched "github.com/Daniel-dev22/agent-kit-go/scheduler"

	"github.com/Daniel-dev22/gdrive-agent/internal/config"
	"github.com/Daniel-dev22/gdrive-agent/internal/job"
)

// Runner is the subset of *job.Runner the scheduler needs. Keeping the
// interface narrow makes the adapter trivial to test with a stub.
type Runner interface {
	StartBackup(ctx context.Context, req job.BackupRequest) (string, error)
}

// New constructs a *kitsched.Scheduler wired to fire gdrive backups for
// each schedule pulled from <CONTROL_CENTER_URL>/api/gdrive/schedules?node=<self>.
// Returns the kit scheduler directly — callers Start / Stop / Pull / Schedules
// / Cache it through the kit's public surface.
//
// CachePath is placed at <BackupRoot>/.agent/schedules.json so the agent's
// operational state lives alongside its data (no extra host bind-mount).
func New(cfg config.Config, routerClient *http.Client, runner Runner) (*kitsched.Scheduler, error) {
	fire := makeGdriveFire(runner)

	return kitsched.New(kitsched.Config{
		PullURL:    fmt.Sprintf("%s/api/gdrive/schedules?node=%s", cfg.ControlCenterURL, cfg.NodeName),
		NodeName:   cfg.NodeName,
		CachePath:  filepath.Join(cfg.BackupRoot, ".agent", "schedules.json"),
		HTTPClient: routerClient,
		Fire:       fire,
	})
}

// makeGdriveFire returns the FireFunc closure that dispatches a job per
// matching schedule. Logs + skips actions the runner doesn't implement
// yet so adding "prune" later only needs a new case.
func makeGdriveFire(runner Runner) kitsched.FireFunc {
	return func(ctx context.Context, sch kitsched.LocalSchedule, triggerKey string, missedRecovery bool) error {
		switch sch.Action {
		case "backup":
			jobID, err := runner.StartBackup(ctx, job.BackupRequest{
				RepoID:     sch.RepoID,
				ScheduleID: sch.ID,
				TriggerKey: triggerKey,
			})
			if err != nil {
				log.Error().Err(err).
					Str("schedule", sch.ID).
					Str("name", sch.Name).
					Str("repo", sch.RepoID).
					Msg("gdrive schedule fire: StartBackup failed")
				return err
			}
			log.Info().
				Str("schedule", sch.ID).
				Str("name", sch.Name).
				Str("job", jobID).
				Str("repo", sch.RepoID).
				Bool("missed_recovery", missedRecovery).
				Msg("gdrive schedule fired (backup)")
			return nil

		case "prune":
			// Prune is allowed by the gdrive_schedules.action CHECK
			// constraint but the job.Runner doesn't expose StartPrune yet.
			// Log + skip so scheduled prune rows don't crash the loop;
			// remove this branch once Runner.StartPrune lands.
			log.Warn().
				Str("schedule", sch.ID).
				Str("name", sch.Name).
				Str("repo", sch.RepoID).
				Msg("gdrive schedule fire: prune action not implemented in runner; skipping")
			return nil

		default:
			log.Warn().
				Str("schedule", sch.ID).
				Str("action", sch.Action).
				Msg("gdrive schedule fire: unknown action; skipping")
			return nil
		}
	}
}
