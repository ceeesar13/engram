package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Gentleman-Programming/engram/internal/cloud"
	"github.com/Gentleman-Programming/engram/internal/cloud/auth"
	"github.com/Gentleman-Programming/engram/internal/cloud/cloudserver"
	"github.com/Gentleman-Programming/engram/internal/cloud/cloudstore"
	"github.com/Gentleman-Programming/engram/internal/cloud/constants"
	"github.com/Gentleman-Programming/engram/internal/cloud/dashboard"
	"github.com/Gentleman-Programming/engram/internal/cloud/remote"
	"github.com/Gentleman-Programming/engram/internal/store"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
)

type cloudManifestReader interface {
	ReadManifest(ctx context.Context, project string) (*engramsync.Manifest, error)
}

type cloudDashboardStatusProvider struct {
	store    cloudManifestReader
	projects []string
}

func (p cloudDashboardStatusProvider) Status() dashboard.SyncStatus {
	if len(p.projects) == 0 {
		return dashboard.SyncStatus{
			Phase:         "degraded",
			ReasonCode:    constants.ReasonBlockedUnenrolled,
			ReasonMessage: "cloud project allowlist is empty",
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	totalChunks := 0
	for _, project := range p.projects {
		manifest, err := p.store.ReadManifest(ctx, project)
		if err != nil {
			log.Printf("[engram] cloud dashboard status manifest read failed for project %q: %v", project, err)
			return dashboard.SyncStatus{
				Phase:         "degraded",
				ReasonCode:    constants.ReasonTransportFailed,
				ReasonMessage: "cloud sync status is temporarily unavailable",
			}
		}
		totalChunks += len(manifest.Chunks)
	}

	return dashboard.SyncStatus{
		Phase:         "healthy",
		ReasonMessage: fmt.Sprintf("cloud chunks available across %d project(s): %d", len(p.projects), totalChunks),
	}
}

type cloudServerRuntime interface {
	Start() error
}

type defaultCloudRuntime struct {
	server *cloudserver.CloudServer
	store  *cloudstore.CloudStore
}

func (r *defaultCloudRuntime) Start() error {
	defer r.store.Close()
	return r.server.Start()
}

var newCloudRuntime = func(cfg cloud.Config) (cloudServerRuntime, error) {
	cs, err := cloudstore.New(cfg)
	if err != nil {
		return nil, err
	}
	allowedProjects := normalizeAllowedProjects(cfg.AllowedProjects)
	if err := backfillAllowedProjectMutationChunks(context.Background(), cs, allowedProjects); err != nil {
		_ = cs.Close()
		return nil, err
	}
	projectAuth := auth.NewProjectScopeAuthorizer(allowedProjects)
	token := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_TOKEN"))
	cs.SetDashboardAllowedProjects(allowedProjects)
	insecureNoAuth := token == "" && envBool("ENGRAM_CLOUD_INSECURE_NO_AUTH")
	var authenticator cloudserver.Authenticator
	if !insecureNoAuth {
		authSvc, err := auth.NewService(cs, cfg.JWTSecret)
		if err != nil {
			_ = cs.Close()
			return nil, err
		}
		authSvc.SetBearerToken(token)
		authSvc.SetAllowedProjects(allowedProjects)
		authSvc.SetDashboardSessionTokens([]string{cfg.AdminToken})
		authenticator = authSvc
	}
	return &defaultCloudRuntime{
		server: cloudserver.New(
			cs,
			authenticator,
			cfg.Port,
			cloudserver.WithHost(cfg.BindHost),
			cloudserver.WithProjectAuthorizer(projectAuth),
			cloudserver.WithDashboardAdminToken(cfg.AdminToken),
			cloudserver.WithMaxPushBodyBytes(cfg.MaxPushBodyBytes),
			cloudserver.WithSyncStatusProvider(cloudDashboardStatusProvider{store: cs, projects: allowedProjects}),
		),
		store: cs,
	}, nil
}

func backfillAllowedProjectMutationChunks(ctx context.Context, cs *cloudstore.CloudStore, projects []string) error {
	for _, project := range projects {
		report, err := cs.BackfillMutationChunks(ctx, project, true)
		if err != nil {
			return fmt.Errorf("cloud repair materialize-mutations for project %q: %w", project, err)
		}
		if report.CandidateMutations > 0 || report.ChunksInserted > 0 {
			fmt.Fprintf(os.Stderr,
				"engram cloud repair materialize-mutations: project=%s candidates=%d already_materialized=%d chunks_planned=%d chunks_inserted=%d\n",
				report.Project, report.CandidateMutations, report.AlreadyMaterialized, report.ChunksPlanned, report.ChunksInserted,
			)
		}
	}
	return nil
}

var runUpgradeBootstrap = func(s *store.Store, project string, cc *cloudConfig) (*engramsync.UpgradeBootstrapResult, error) {
	transport, err := remote.NewRemoteTransport(cc.ServerURL, cc.Token, project)
	if err != nil {
		return nil, err
	}
	return engramsync.BootstrapProject(s, transport, engramsync.UpgradeBootstrapOptions{Project: project, CreatedBy: "engram-cloud-upgrade"})
}

type cloudConfig struct {
	// Legacy single-destination fields (v1). Retained so existing installs and
	// `engram cloud config --server` keep working; migrated into a `default`
	// remote at runtime.
	ServerURL string `json:"server_url,omitempty"`
	Token     string `json:"token,omitempty"`

	// v2: named cloud destinations and the per-project routing table.
	Remotes       map[string]remoteConfig `json:"remotes,omitempty"`
	Routes        map[string]string       `json:"routes,omitempty"`
	DefaultRemote string                  `json:"default_remote,omitempty"`
}

// remoteConfig is a single named cloud destination (server URL + bearer token).
type remoteConfig struct {
	ServerURL string `json:"server_url"`
	Token     string `json:"token,omitempty"`
}

const legacyDefaultRemoteName = "default"

// clone returns a deep copy safe to mutate without touching the receiver.
func (cc *cloudConfig) clone() *cloudConfig {
	out := &cloudConfig{
		ServerURL:     cc.ServerURL,
		Token:         cc.Token,
		DefaultRemote: cc.DefaultRemote,
	}
	if cc.Remotes != nil {
		out.Remotes = make(map[string]remoteConfig, len(cc.Remotes))
		for k, v := range cc.Remotes {
			out.Remotes[k] = v
		}
	}
	if cc.Routes != nil {
		out.Routes = make(map[string]string, len(cc.Routes))
		for k, v := range cc.Routes {
			out.Routes[k] = v
		}
	}
	return out
}

// migrateLegacy folds v1 top-level server_url/token into a `default` remote so
// existing single-cloud installs keep their previous behavior with no action.
func (cc *cloudConfig) migrateLegacy() {
	if strings.TrimSpace(cc.ServerURL) == "" && strings.TrimSpace(cc.Token) == "" {
		return
	}
	if cc.Remotes == nil {
		cc.Remotes = map[string]remoteConfig{}
	}
	if _, exists := cc.Remotes[legacyDefaultRemoteName]; !exists {
		cc.Remotes[legacyDefaultRemoteName] = remoteConfig{
			ServerURL: strings.TrimSpace(cc.ServerURL),
			Token:     strings.TrimSpace(cc.Token),
		}
	}
	if strings.TrimSpace(cc.DefaultRemote) == "" {
		cc.DefaultRemote = legacyDefaultRemoteName
	}
	cc.ServerURL = ""
	cc.Token = ""
}

// applyEnvOverrides folds ENGRAM_CLOUD_SERVER / ENGRAM_CLOUD_TOKEN into the
// default remote, preserving the historical env-only escape hatch. It operates
// on a runtime clone and never persists env-provided secrets to disk.
//
// Security: this must never defeat per-project default-deny. If remotes/routes
// are configured but DefaultRemote is intentionally empty (strict-deny), env
// vars must NOT manufacture a default — that would let an unrouted project
// silently sync to the env-provided server instead of being denied. Env
// overrides only ever (a) fold into an already-configured default remote, or
// (b) create the `default` remote when there are no remotes at all (pure
// legacy / env-only setups, preserving issues #343/#421).
func (cc *cloudConfig) applyEnvOverrides() {
	srv := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_SERVER"))
	tok := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_TOKEN"))
	if srv == "" && tok == "" {
		return
	}
	name := strings.TrimSpace(cc.DefaultRemote)
	if name == "" {
		if len(cc.Remotes) > 0 {
			// Strict-deny config: remotes exist but no default was chosen.
			// Do not manufacture one from env — that would bypass default-deny.
			return
		}
		name = legacyDefaultRemoteName
	}
	if cc.Remotes == nil {
		cc.Remotes = map[string]remoteConfig{}
	}
	rc := cc.Remotes[name]
	if srv != "" {
		rc.ServerURL = srv
	}
	if tok != "" {
		rc.Token = tok
	}
	cc.Remotes[name] = rc
	if strings.TrimSpace(cc.DefaultRemote) == "" {
		cc.DefaultRemote = name
	}
}

// normalizedForRuntime returns a runtime view (v2 shape, env overrides applied)
// safe to read for destination resolution. The result MUST NOT be persisted.
func (cc *cloudConfig) normalizedForRuntime() *cloudConfig {
	rt := cc.clone()
	rt.migrateLegacy()
	rt.applyEnvOverrides()
	return rt
}

// routeRemote returns the remote name a project resolves to: its explicit route
// if set, otherwise the configured default remote. Empty means no route applies.
func (cc *cloudConfig) routeRemote(project string) string {
	project, _ = store.NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project != "" && cc.Routes != nil {
		if name, ok := cc.Routes[project]; ok && strings.TrimSpace(name) != "" {
			return strings.TrimSpace(name)
		}
	}
	return strings.TrimSpace(cc.DefaultRemote)
}

// resolveForProject returns the effective destination for a project applying
// per-project routing with default-deny. On error, reason is a deterministic
// reason code (blocked_no_route or cloud_config_error).
func (cc *cloudConfig) resolveForProject(project string) (effective cloudConfig, remoteName, reason string, err error) {
	remoteName = cc.routeRemote(project)
	proj := strings.TrimSpace(project)
	if remoteName == "" {
		if len(cc.Remotes) == 0 {
			// Cloud was never configured — preserve the historical
			// missing-server error rather than reporting a routing denial.
			return cloudConfig{}, "", constants.ReasonCloudConfigError, fmt.Errorf(
				"cloud server is missing: configure server URL with `engram cloud config --server <url>` or add a remote with `engram cloud remote add <name> --server <url>`",
			)
		}
		if proj == "" {
			return cloudConfig{}, "", constants.ReasonBlockedNoRoute, fmt.Errorf("no project specified for cloud sync")
		}
		return cloudConfig{}, "", constants.ReasonBlockedNoRoute, fmt.Errorf(
			"project %q has no cloud route; assign one with `engram cloud route set --project %s --remote <name>` or set a default with `engram cloud remote set-default <name>`",
			proj, proj,
		)
	}
	rc, ok := cc.Remotes[remoteName]
	if !ok {
		return cloudConfig{}, remoteName, constants.ReasonCloudConfigError, fmt.Errorf(
			"cloud route for project %q points to unknown remote %q; add it with `engram cloud remote add %s --server <url>`",
			proj, remoteName, remoteName,
		)
	}
	return cloudConfig{ServerURL: rc.ServerURL, Token: rc.Token}, remoteName, "", nil
}

func cmdCloud(cfg store.Config) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: engram cloud <subcommand> [options]")
		fmt.Fprintln(os.Stderr, "supported subcommands: status, enroll, config, remote, route, serve, upgrade, repair")
		exitFunc(1)
	}
	if os.Args[2] == "--help" || os.Args[2] == "-h" || os.Args[2] == "help" {
		fmt.Println("usage: engram cloud <subcommand> [options]")
		fmt.Println("supported subcommands: status, enroll, config, remote, route, serve, upgrade, repair")
		return
	}

	switch os.Args[2] {
	case "status":
		cmdCloudStatus(cfg)
	case "enroll":
		cmdCloudEnroll(cfg)
	case "config":
		cmdCloudConfig(cfg)
	case "remote":
		cmdCloudRemote(cfg)
	case "route":
		cmdCloudRoute(cfg)
	case "serve":
		cmdCloudServe()
	case "upgrade":
		cmdCloudUpgrade(cfg)
	case "repair":
		cmdCloudRepair()
	default:
		fmt.Fprintf(os.Stderr, "unknown cloud command: %s\n", os.Args[2])
		fmt.Fprintln(os.Stderr, "supported subcommands: status, enroll, config, remote, route, serve, upgrade, repair")
		exitFunc(1)
	}
}

func cmdCloudRepair() {
	if len(os.Args) < 4 || os.Args[3] == "--help" || os.Args[3] == "-h" || os.Args[3] == "help" {
		fmt.Println("usage: engram cloud repair materialize-mutations --project <name> (--dry-run|--apply)")
		fmt.Println("repairs existing cloud_mutations into compatible cloud_chunks without deleting remote data")
		return
	}
	command := strings.TrimSpace(strings.ToLower(os.Args[3]))
	if command != "materialize-mutations" {
		fmt.Fprintf(os.Stderr, "unknown cloud repair command: %s\n", command)
		fmt.Fprintln(os.Stderr, "supported cloud repair commands: materialize-mutations")
		exitFunc(1)
		return
	}
	project := parseCloudUpgradeProjectArg(os.Args[4:])
	if project == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud repair materialize-mutations --project <name> (--dry-run|--apply)")
		fmt.Fprintln(os.Stderr, "error: --project is required")
		exitFunc(1)
		return
	}
	dryRun := hasCloudUpgradeFlag(os.Args[4:], "--dry-run")
	apply := hasCloudUpgradeFlag(os.Args[4:], "--apply")
	if dryRun == apply {
		fmt.Fprintln(os.Stderr, "usage: engram cloud repair materialize-mutations --project <name> (--dry-run|--apply)")
		fmt.Fprintln(os.Stderr, "error: exactly one of --dry-run or --apply is required")
		exitFunc(1)
		return
	}

	cs, err := cloudstore.New(cloud.ConfigFromEnv())
	if err != nil {
		fatal(err)
		return
	}
	defer cs.Close()
	report, err := cs.BackfillMutationChunks(context.Background(), project, apply)
	if err != nil {
		fatal(err)
		return
	}
	encoded, err := jsonMarshalIndent(report, "", "  ")
	if err != nil {
		fatal(err)
		return
	}
	fmt.Println(string(encoded))
}

func cmdCloudUpgrade(cfg store.Config) {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: engram cloud upgrade <doctor|repair|bootstrap|status|rollback> --project <name>")
		exitFunc(1)
		return
	}
	command := strings.TrimSpace(strings.ToLower(os.Args[3]))
	if command == "--help" || command == "-h" || command == "help" {
		fmt.Println("engram cloud upgrade")
		fmt.Println("workflow: doctor -> repair -> bootstrap -> status/rollback")
		fmt.Println("cloud is opt-in replication/shared access; local SQLite remains source of truth")
		fmt.Println("usage: engram cloud upgrade <doctor|repair|bootstrap|status|rollback> --project <name>")
		return
	}
	switch command {
	case "doctor":
		cmdCloudUpgradeDoctor(cfg)
	case "repair":
		cmdCloudUpgradeRepair(cfg)
	case "bootstrap":
		cmdCloudUpgradeBootstrap(cfg)
	case "status":
		cmdCloudUpgradeStatus(cfg)
	case "rollback":
		cmdCloudUpgradeRollback(cfg)
	default:
		fmt.Fprintf(os.Stderr, "unknown cloud upgrade command: %s\n", command)
		fmt.Fprintln(os.Stderr, "supported cloud upgrade commands: doctor, repair, bootstrap, status, rollback")
		exitFunc(1)
	}
}

func cmdCloudUpgradeDoctor(cfg store.Config) {
	project := parseCloudUpgradeProjectArg(os.Args[4:])
	if project == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud upgrade doctor --project <name>")
		fmt.Fprintln(os.Stderr, "error: --project is required")
		exitFunc(1)
		return
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()

	cloudConfigured := false
	if cc, _, _, cfgErr := resolveRoutedCloudConfig(cfg, project); cfgErr == nil {
		if cc != nil {
			if validated, err := validateCloudServerURL(cc.ServerURL); err == nil && strings.TrimSpace(validated) != "" {
				cloudConfigured = true
			}
		}
	}
	enrolled, err := s.IsProjectEnrolled(project)
	if err != nil {
		fatal(fmt.Errorf("cloud upgrade doctor enrollment check: %w", err))
		return
	}
	policyDenied, err := cloudUpgradePolicyDenied(s, project)
	if err != nil {
		fatal(fmt.Errorf("cloud upgrade doctor policy check: %w", err))
		return
	}

	report, err := engramsync.DiagnoseCloudUpgrade(engramsync.UpgradeDiagnosisInput{
		Project:         project,
		CloudConfigured: cloudConfigured,
		ProjectEnrolled: enrolled,
		PolicyDenied:    policyDenied,
	})
	if err != nil {
		fatal(err)
		return
	}

	legacyReport, err := s.DiagnoseCloudUpgradeLegacyMutations(project)
	if err != nil {
		fatal(fmt.Errorf("cloud upgrade doctor legacy mutation diagnosis: %w", err))
		return
	}
	if legacyReport.BlockedCount > 0 {
		first := legacyReport.Findings[0]
		report = engramsync.UpgradeDiagnosisReport{
			Status:  engramsync.UpgradeStatusBlocked,
			Class:   engramsync.UpgradeReasonClassBlocked,
			Code:    store.UpgradeReasonBlockedLegacyMutationManual,
			Message: fmt.Sprintf("manual-action-required: %s (seq=%d entity=%s op=%s)", first.Message, first.Seq, first.Entity, first.Op),
		}
	} else if legacyReport.RepairableCount > 0 {
		report = engramsync.UpgradeDiagnosisReport{
			Status:  engramsync.UpgradeStatusBlocked,
			Class:   engramsync.UpgradeReasonClassRepairable,
			Code:    store.UpgradeReasonRepairableLegacyMutationPayload,
			Message: fmt.Sprintf("project %q has %d repairable legacy mutation payload issue(s); run `engram cloud upgrade repair --project %s --apply`", project, legacyReport.RepairableCount, project),
		}
	}

	stage := store.UpgradeStageDoctorBlocked
	if report.Status == engramsync.UpgradeStatusReady {
		stage = store.UpgradeStageDoctorReady
	}
	_ = s.SaveCloudUpgradeState(store.CloudUpgradeState{
		Project:          project,
		Stage:            stage,
		RepairClass:      report.Class,
		LastErrorCode:    report.Code,
		LastErrorMessage: report.Message,
	})

	fmt.Printf("project: %s\n", project)
	fmt.Printf("status: %s\n", report.Status)
	fmt.Printf("class: %s\n", report.Class)
	fmt.Printf("reason_code: %s\n", report.Code)
	fmt.Printf("message: %s\n", report.Message)
}

func cloudUpgradePolicyDenied(s *store.Store, project string) (bool, error) {
	targets := []string{cloudTargetKeyForProject(project)}
	if cloudTargetKeyForProject(project) != constants.TargetKeyCloud {
		targets = append(targets, constants.TargetKeyCloud)
	}
	for _, targetKey := range targets {
		state, err := s.GetSyncState(targetKey)
		if err != nil {
			return false, err
		}
		if state == nil {
			continue
		}
		if strings.TrimSpace(derefString(state.ReasonCode)) == constants.ReasonPolicyForbidden {
			return true, nil
		}
	}
	return false, nil
}

func parseCloudUpgradeProjectArg(args []string) string {
	for i := 0; i < len(args); i++ {
		if strings.TrimSpace(args[i]) != "--project" {
			continue
		}
		if i+1 >= len(args) {
			return ""
		}
		project, _ := store.NormalizeProject(args[i+1])
		return strings.TrimSpace(project)
	}
	return ""
}

func cmdCloudUpgradeRepair(cfg store.Config) {
	project := parseCloudUpgradeProjectArg(os.Args[4:])
	if project == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud upgrade repair --project <name> [--dry-run|--apply]")
		fmt.Fprintln(os.Stderr, "error: --project is required")
		exitFunc(1)
		return
	}
	apply := hasCloudUpgradeFlag(os.Args[4:], "--apply")
	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()
	report, err := s.RepairCloudUpgrade(project, apply)
	if err != nil {
		fatal(err)
		return
	}
	fmt.Printf("project: %s\n", project)
	fmt.Printf("class: %s\n", report.Class)
	fmt.Printf("reason_code: %s\n", report.ReasonCode)
	fmt.Printf("message: %s\n", report.Message)
	fmt.Printf("applied: %t\n", report.Applied)
}

func cmdCloudUpgradeBootstrap(cfg store.Config) {
	project := parseCloudUpgradeProjectArg(os.Args[4:])
	if project == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud upgrade bootstrap --project <name> [--resume]")
		fmt.Fprintln(os.Stderr, "error: --project is required")
		exitFunc(1)
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()

	cc, _, _, err := resolveRoutedCloudConfig(cfg, project)
	if err != nil {
		fatal(err)
		return
	}
	if cc == nil || strings.TrimSpace(cc.ServerURL) == "" {
		fatal(fmt.Errorf("cloud upgrade bootstrap requires configured cloud server"))
		return
	}
	validatedURL, err := validateCloudServerURL(cc.ServerURL)
	if err != nil {
		fatal(fmt.Errorf("invalid cloud runtime server URL: %w", err))
		return
	}
	cc.ServerURL = validatedURL
	if err := captureUpgradeSnapshotBeforeBootstrap(s, cfg, project); err != nil {
		fatal(err)
		return
	}
	legacyReport, err := s.DiagnoseCloudUpgradeLegacyMutations(project)
	if err != nil {
		fatal(fmt.Errorf("cloud upgrade bootstrap legacy mutation diagnosis: %w", err))
		return
	}
	if legacyReport.BlockedCount > 0 {
		first := legacyReport.Findings[0]
		fatal(fmt.Errorf("legacy mutation payloads require manual action before bootstrap (seq=%d entity=%s op=%s): %s", first.Seq, first.Entity, first.Op, first.Message))
		return
	}
	if legacyReport.RepairableCount > 0 {
		fatal(fmt.Errorf("legacy mutation payloads require repair before bootstrap: run `engram cloud upgrade repair --project %s --apply`", project))
		return
	}

	result, err := runUpgradeBootstrap(s, project, cc)
	if err != nil {
		fatal(err)
		return
	}
	fmt.Printf("project: %s\n", project)
	fmt.Printf("stage: %s\n", result.Stage)
	fmt.Printf("resumed: %t\n", result.Resumed)
	fmt.Printf("noop: %t\n", result.NoOp)
}

func captureUpgradeSnapshotBeforeBootstrap(s *store.Store, cfg store.Config, project string) error {
	state, err := s.GetCloudUpgradeState(project)
	if err != nil {
		return fmt.Errorf("load cloud upgrade state before bootstrap snapshot: %w", err)
	}
	if state != nil {
		snapshot := state.Snapshot
		if snapshot.CloudConfigPresent || strings.TrimSpace(snapshot.CloudConfigJSON) != "" || snapshot.ProjectEnrolled {
			return nil
		}
	}

	enrolled, err := s.IsProjectEnrolled(project)
	if err != nil {
		return fmt.Errorf("load project enrollment before bootstrap snapshot: %w", err)
	}

	var snapshot store.CloudUpgradeSnapshot
	configBytes, err := os.ReadFile(cloudConfigPath(cfg))
	if err == nil {
		snapshot.CloudConfigPresent = true
		snapshot.CloudConfigJSON = string(configBytes)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read cloud config for bootstrap snapshot: %w", err)
	}
	snapshot.ProjectEnrolled = enrolled

	next := store.CloudUpgradeState{Project: project, Stage: store.UpgradeStagePlanned, RepairClass: store.UpgradeRepairClassNone, Snapshot: snapshot}
	if state != nil {
		next = *state
		next.Snapshot = snapshot
	}
	if err := s.SaveCloudUpgradeState(next); err != nil {
		return fmt.Errorf("persist pre-bootstrap rollback snapshot: %w", err)
	}
	return nil
}

func cmdCloudUpgradeStatus(cfg store.Config) {
	project := parseCloudUpgradeProjectArg(os.Args[4:])
	if project == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud upgrade status --project <name>")
		fmt.Fprintln(os.Stderr, "error: --project is required")
		exitFunc(1)
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()
	state, err := s.GetCloudUpgradeState(project)
	if err != nil {
		fatal(err)
		return
	}
	if state == nil {
		fmt.Printf("project: %s\n", project)
		fmt.Printf("stage: %s\n", store.UpgradeStagePlanned)
		return
	}
	fmt.Printf("project: %s\n", project)
	fmt.Printf("stage: %s\n", state.Stage)
	fmt.Printf("class: %s\n", state.RepairClass)
	fmt.Printf("reason_code: %s\n", strings.TrimSpace(state.LastErrorCode))
	fmt.Printf("reason_message: %s\n", strings.TrimSpace(state.LastErrorMessage))
}

func cmdCloudUpgradeRollback(cfg store.Config) {
	project := parseCloudUpgradeProjectArg(os.Args[4:])
	if project == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud upgrade rollback --project <name>")
		fmt.Fprintln(os.Stderr, "error: --project is required")
		exitFunc(1)
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()
	state, err := s.GetCloudUpgradeState(project)
	if err != nil {
		fatal(err)
		return
	}
	if state == nil {
		fatal(fmt.Errorf("rollback requires existing upgrade checkpoint state"))
		return
	}
	canRollback, err := s.CanRollbackCloudUpgrade(project)
	if err != nil {
		fatal(err)
		return
	}
	if !canRollback {
		fmt.Fprintln(os.Stderr, "rollback is unavailable post-bootstrap; use explicit disconnect/unenroll flows")
		exitFunc(1)
		return
	}
	if state.Snapshot.CloudConfigPresent {
		if err := os.WriteFile(cloudConfigPath(cfg), []byte(state.Snapshot.CloudConfigJSON), 0o600); err != nil {
			fatal(err)
			return
		}
	} else {
		_ = os.Remove(cloudConfigPath(cfg))
	}
	rolledBack, err := engramsync.RollbackProject(s, engramsync.UpgradeRollbackOptions{Project: project})
	if err != nil {
		fatal(err)
		return
	}
	fmt.Printf("project: %s\n", project)
	fmt.Printf("stage: %s\n", rolledBack.Stage)
}

func hasCloudUpgradeFlag(args []string, flag string) bool {
	for _, arg := range args {
		if strings.TrimSpace(arg) == flag {
			return true
		}
	}
	return false
}

func cmdCloudStatus(cfg store.Config) {
	cc, err := resolveCloudRuntimeConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: unable to read cloud runtime config: %v\n", err)
		exitFunc(1)
		return
	}
	if cc == nil || cc.ServerURL == "" {
		fmt.Println("Cloud status: not configured")
		return
	}
	validatedURL, err := validateCloudServerURL(cc.ServerURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid cloud runtime server URL: %v\n", err)
		exitFunc(1)
		return
	}
	cc.ServerURL = validatedURL
	token := strings.TrimSpace(cc.Token)
	insecureNoAuth := envBool("ENGRAM_CLOUD_INSECURE_NO_AUTH")
	fmt.Printf("Cloud status: configured (target=%s)\n", constants.TargetKeyCloud)
	fmt.Printf("Server: %s\n", cc.ServerURL)
	if token == "" {
		if insecureNoAuth {
			fmt.Println("Auth status: ready (insecure local-dev mode: ENGRAM_CLOUD_INSECURE_NO_AUTH=1)")
			fmt.Println("Sync readiness: ready for explicit --project sync (project must be enrolled)")
			fmt.Println("Warning: bearer auth is disabled in insecure mode; do not use in production")
			printCloudStatusDaemonProbe()
			printCloudStatusSyncDiagnostic(cfg)
			return
		}
		fmt.Println("Auth status: token not configured (client token is optional at preflight)")
		fmt.Println("Sync readiness: ready to attempt explicit --project sync (project must be enrolled)")
		fmt.Println("Hint: if the remote server enforces bearer auth, set ENGRAM_CLOUD_TOKEN")
		printCloudStatusDaemonProbe()
		printCloudStatusSyncDiagnostic(cfg)
		return
	}
	fmt.Println("Auth status: ready (token provided via runtime cloud config)")
	fmt.Println("Sync readiness: ready for explicit --project sync (project must be enrolled)")
	printCloudStatusDaemonProbe()
	printCloudStatusSyncDiagnostic(cfg)
}

func printCloudStatusSyncDiagnostic(cfg store.Config) {
	if _, err := os.Stat(filepath.Join(cfg.DataDir, "engram.db")); err != nil {
		return
	}
	s, err := storeNew(cfg)
	if err != nil {
		fmt.Printf("Sync diagnostic: unavailable (%v)\n", err)
		return
	}
	defer s.Close()
	state, err := s.GetSyncState(constants.TargetKeyCloud)
	if err != nil || state == nil {
		return
	}
	code := strings.TrimSpace(derefString(state.ReasonCode))
	message := strings.TrimSpace(derefString(state.ReasonMessage))
	if code == "" && message == "" {
		return
	}
	fmt.Printf("Sync diagnostic: %s\n", state.Lifecycle)
	if code != "" {
		fmt.Printf("reason_code: %s\n", code)
	}
	if message != "" {
		fmt.Printf("reason_message: %s\n", message)
	}
}

func cmdCloudEnroll(cfg store.Config) {
	if len(os.Args) >= 4 {
		arg := strings.TrimSpace(os.Args[3])
		if arg == "--help" || arg == "-h" || arg == "help" {
			fmt.Println("usage: engram cloud enroll <project>")
			fmt.Println("Enroll a local-first project for explicit cloud replication.")
			return
		}
	}
	if len(os.Args) < 4 || strings.TrimSpace(os.Args[3]) == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud enroll <project>")
		exitFunc(1)
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()

	projectName := strings.TrimSpace(os.Args[3])
	if err := s.EnrollProject(projectName); err != nil {
		fatal(err)
		return
	}

	fmt.Printf("✓ Project %q enrolled for cloud sync\n", projectName)
}

func cmdCloudConfig(cfg store.Config) {
	if len(os.Args) < 5 || os.Args[3] != "--server" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud config --server <url>")
		exitFunc(1)
	}
	serverURL := strings.TrimSpace(os.Args[4])
	if serverURL == "" {
		fmt.Fprintln(os.Stderr, "error: server URL is required")
		exitFunc(1)
	}
	validatedURL, err := validateCloudServerURL(serverURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid server URL: %v\n", err)
		exitFunc(1)
	}
	// Preserve any configured v2 remotes/routes; only update the legacy
	// single-destination server URL (migrated into the default remote at runtime).
	cc, err := loadCloudConfig(cfg)
	if err != nil {
		fatal(err)
		return
	}
	if cc == nil {
		cc = &cloudConfig{}
	}
	cc.ServerURL = validatedURL
	// v2: once a `default` remote already exists, migrateLegacy's `if !exists`
	// guard means the legacy ServerURL field above is otherwise ignored at
	// runtime. Keep `cloud config --server` working post-migration by also
	// updating the active default remote's ServerURL (preserving its token).
	//
	// Security: if remotes are configured but no default is set (strict
	// default-deny: named remotes + routes, no default), we must NOT
	// manufacture a `default` remote here — that would silently give an
	// otherwise-denied unrouted project a destination to sync to. Refuse
	// instead and point the operator at the explicit remote/route commands.
	if len(cc.Remotes) > 0 {
		name := strings.TrimSpace(cc.DefaultRemote)
		if name == "" {
			fmt.Fprintln(os.Stderr, "error: named remotes are configured without a default remote; `cloud config --server` cannot pick a target without disabling default-deny. Use `engram cloud remote add <name> --server <url>`, `engram cloud remote set-default <name>`, or `engram cloud route set --project <p> --remote <name>`.")
			exitFunc(1)
			return
		}
		rc := cc.Remotes[name]
		rc.ServerURL = validatedURL
		cc.Remotes[name] = rc
	}
	if err := saveCloudConfig(cfg, cc); err != nil {
		fatal(err)
		return
	}
	fmt.Printf("✓ Cloud server set to %s\n", cc.ServerURL)
}

// loadCloudConfigForWrite loads cloud.json for mutation, folding any legacy
// single-destination fields into the `default` remote so config commands
// converge the file on the v2 shape without dropping the existing destination.
func loadCloudConfigForWrite(cfg store.Config) *cloudConfig {
	cc, err := loadCloudConfig(cfg)
	if err != nil {
		fatal(err)
		return &cloudConfig{}
	}
	if cc == nil {
		cc = &cloudConfig{}
	}
	cc.migrateLegacy()
	return cc
}

// parseCloudFlags parses a flat list of `--key value` pairs. Malformed input
// returns an error naming the offending token.
func parseCloudFlags(args []string) (map[string]string, error) {
	out := map[string]string{}
	for i := 0; i < len(args); i++ {
		key := args[i]
		if !strings.HasPrefix(key, "--") {
			return nil, fmt.Errorf("unexpected argument %q", key)
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("flag %q requires a value", key)
		}
		out[strings.TrimPrefix(key, "--")] = strings.TrimSpace(args[i+1])
		i++
	}
	return out, nil
}

func cloudRemoteUsage() {
	fmt.Fprintln(os.Stderr, "usage: engram cloud remote <add|list|remove|set-default> [options]")
	fmt.Fprintln(os.Stderr, "  add <name> --server <url> [--token <token>]")
	fmt.Fprintln(os.Stderr, "  list")
	fmt.Fprintln(os.Stderr, "  remove <name>")
	fmt.Fprintln(os.Stderr, "  set-default <name>")
}

func cmdCloudRemote(cfg store.Config) {
	if len(os.Args) < 4 {
		cloudRemoteUsage()
		exitFunc(1)
		return
	}
	switch os.Args[3] {
	case "add":
		cmdCloudRemoteAdd(cfg)
	case "list", "ls":
		cmdCloudRemoteList(cfg)
	case "remove", "rm":
		cmdCloudRemoteRemove(cfg)
	case "set-default":
		cmdCloudRemoteSetDefault(cfg)
	case "--help", "-h", "help":
		cloudRemoteUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown cloud remote command: %s\n", os.Args[3])
		cloudRemoteUsage()
		exitFunc(1)
	}
}

func cmdCloudRemoteAdd(cfg store.Config) {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: engram cloud remote add <name> --server <url> [--token <token>]")
		exitFunc(1)
		return
	}
	name := strings.TrimSpace(os.Args[4])
	if name == "" || strings.HasPrefix(name, "--") {
		fmt.Fprintln(os.Stderr, "error: remote name is required")
		exitFunc(1)
		return
	}
	flags, err := parseCloudFlags(os.Args[5:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exitFunc(1)
		return
	}
	server := flags["server"]
	if server == "" {
		fmt.Fprintln(os.Stderr, "error: --server <url> is required")
		exitFunc(1)
		return
	}
	validatedURL, err := validateCloudServerURL(server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid server URL: %v\n", err)
		exitFunc(1)
		return
	}
	cc := loadCloudConfigForWrite(cfg)
	if cc.Remotes == nil {
		cc.Remotes = map[string]remoteConfig{}
	}
	cc.Remotes[name] = remoteConfig{ServerURL: validatedURL, Token: strings.TrimSpace(flags["token"])}
	if err := saveCloudConfig(cfg, cc); err != nil {
		fatal(err)
		return
	}
	fmt.Printf("✓ Cloud remote %q set to %s\n", name, validatedURL)
	if strings.TrimSpace(cc.DefaultRemote) == "" {
		fmt.Printf("  note: no default remote set; route projects with `engram cloud route set` or run `engram cloud remote set-default %s`\n", name)
	}
}

func cmdCloudRemoteList(cfg store.Config) {
	cc := loadCloudConfigForWrite(cfg)
	if len(cc.Remotes) == 0 {
		fmt.Println("No cloud remotes configured.")
		fmt.Println("Add one with `engram cloud remote add <name> --server <url> [--token <token>]`")
		return
	}
	names := make([]string, 0, len(cc.Remotes))
	for name := range cc.Remotes {
		names = append(names, name)
	}
	sort.Strings(names)
	fmt.Println("Cloud remotes:")
	for _, name := range names {
		rc := cc.Remotes[name]
		marker := " "
		if name == strings.TrimSpace(cc.DefaultRemote) {
			marker = "*"
		}
		tokenState := "no token"
		if strings.TrimSpace(rc.Token) != "" {
			tokenState = "token set"
		}
		fmt.Printf("  %s %-16s %s (%s)\n", marker, name, rc.ServerURL, tokenState)
	}
	if strings.TrimSpace(cc.DefaultRemote) != "" {
		fmt.Printf("Default remote: %s\n", cc.DefaultRemote)
	} else {
		fmt.Println("Default remote: (none — projects without an explicit route will not sync)")
	}
}

func cmdCloudRemoteRemove(cfg store.Config) {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: engram cloud remote remove <name>")
		exitFunc(1)
		return
	}
	name := strings.TrimSpace(os.Args[4])
	cc := loadCloudConfigForWrite(cfg)
	if _, ok := cc.Remotes[name]; !ok {
		fmt.Fprintf(os.Stderr, "error: remote %q does not exist\n", name)
		exitFunc(1)
		return
	}
	var orphaned []string
	for project, remoteName := range cc.Routes {
		if remoteName == name {
			orphaned = append(orphaned, project)
		}
	}
	if len(orphaned) > 0 {
		sort.Strings(orphaned)
		fmt.Fprintf(os.Stderr, "error: remote %q is still routed by project(s): %s\n", name, strings.Join(orphaned, ", "))
		fmt.Fprintln(os.Stderr, "unset those routes first with `engram cloud route unset --project <name>`")
		exitFunc(1)
		return
	}
	delete(cc.Remotes, name)
	if strings.TrimSpace(cc.DefaultRemote) == name {
		cc.DefaultRemote = ""
	}
	if err := saveCloudConfig(cfg, cc); err != nil {
		fatal(err)
		return
	}
	fmt.Printf("✓ Cloud remote %q removed\n", name)
}

func cmdCloudRemoteSetDefault(cfg store.Config) {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: engram cloud remote set-default <name>")
		exitFunc(1)
		return
	}
	name := strings.TrimSpace(os.Args[4])
	cc := loadCloudConfigForWrite(cfg)
	if name != "" {
		if _, ok := cc.Remotes[name]; !ok {
			fmt.Fprintf(os.Stderr, "error: remote %q does not exist; add it first with `engram cloud remote add`\n", name)
			exitFunc(1)
			return
		}
	}
	cc.DefaultRemote = name
	if err := saveCloudConfig(cfg, cc); err != nil {
		fatal(err)
		return
	}
	if name == "" {
		fmt.Println("✓ Default remote cleared (projects without an explicit route will not sync)")
	} else {
		fmt.Printf("✓ Default remote set to %q\n", name)
	}
}

func cloudRouteUsage() {
	fmt.Fprintln(os.Stderr, "usage: engram cloud route <set|list|unset> [options]")
	fmt.Fprintln(os.Stderr, "  set --project <project> --remote <name>")
	fmt.Fprintln(os.Stderr, "  list")
	fmt.Fprintln(os.Stderr, "  unset --project <project>")
}

func cmdCloudRoute(cfg store.Config) {
	if len(os.Args) < 4 {
		cloudRouteUsage()
		exitFunc(1)
		return
	}
	switch os.Args[3] {
	case "set":
		cmdCloudRouteSet(cfg)
	case "list", "ls":
		cmdCloudRouteList(cfg)
	case "unset", "remove", "rm":
		cmdCloudRouteUnset(cfg)
	case "--help", "-h", "help":
		cloudRouteUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown cloud route command: %s\n", os.Args[3])
		cloudRouteUsage()
		exitFunc(1)
	}
}

func cmdCloudRouteSet(cfg store.Config) {
	flags, err := parseCloudFlags(os.Args[4:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exitFunc(1)
		return
	}
	project, _ := store.NormalizeProject(flags["project"])
	project = strings.TrimSpace(project)
	remoteName := strings.TrimSpace(flags["remote"])
	if project == "" || remoteName == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud route set --project <project> --remote <name>")
		exitFunc(1)
		return
	}
	cc := loadCloudConfigForWrite(cfg)
	if _, ok := cc.Remotes[remoteName]; !ok {
		fmt.Fprintf(os.Stderr, "error: remote %q does not exist; add it first with `engram cloud remote add %s --server <url>`\n", remoteName, remoteName)
		exitFunc(1)
		return
	}
	if cc.Routes == nil {
		cc.Routes = map[string]string{}
	}
	cc.Routes[project] = remoteName
	if err := saveCloudConfig(cfg, cc); err != nil {
		fatal(err)
		return
	}
	fmt.Printf("✓ Project %q routed to remote %q\n", project, remoteName)
}

func cmdCloudRouteList(cfg store.Config) {
	cc := loadCloudConfigForWrite(cfg)
	if len(cc.Routes) == 0 {
		fmt.Println("No project routes configured.")
		if strings.TrimSpace(cc.DefaultRemote) != "" {
			fmt.Printf("Default remote (fallback for unrouted projects): %s\n", cc.DefaultRemote)
		} else {
			fmt.Println("No default remote: projects without a route will not sync (default-deny).")
		}
		return
	}
	projects := make([]string, 0, len(cc.Routes))
	for project := range cc.Routes {
		projects = append(projects, project)
	}
	sort.Strings(projects)
	fmt.Println("Project routes:")
	for _, project := range projects {
		remoteName := cc.Routes[project]
		state := ""
		if _, ok := cc.Remotes[remoteName]; !ok {
			state = "  [!] unknown remote"
		}
		fmt.Printf("  %-20s -> %s%s\n", project, remoteName, state)
	}
	if strings.TrimSpace(cc.DefaultRemote) != "" {
		fmt.Printf("Default remote (fallback for unrouted projects): %s\n", cc.DefaultRemote)
	} else {
		fmt.Println("No default remote: projects without a route will not sync (default-deny).")
	}
}

func cmdCloudRouteUnset(cfg store.Config) {
	flags, err := parseCloudFlags(os.Args[4:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		exitFunc(1)
		return
	}
	project, _ := store.NormalizeProject(flags["project"])
	project = strings.TrimSpace(project)
	if project == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud route unset --project <project>")
		exitFunc(1)
		return
	}
	cc := loadCloudConfigForWrite(cfg)
	if _, ok := cc.Routes[project]; !ok {
		fmt.Printf("Project %q has no route.\n", project)
		return
	}
	delete(cc.Routes, project)
	if err := saveCloudConfig(cfg, cc); err != nil {
		fatal(err)
		return
	}
	fmt.Printf("✓ Route for project %q removed\n", project)
}

func validateCloudServerURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.ParseRequestURI(trimmed)
	if err != nil {
		return "", err
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("scheme must be http or https")
	}
	if strings.TrimSpace(parsed.Host) == "" || strings.TrimSpace(parsed.Hostname()) == "" {
		return "", fmt.Errorf("host is required")
	}
	if strings.TrimSpace(parsed.RawQuery) != "" {
		return "", fmt.Errorf("query is not allowed")
	}
	if strings.TrimSpace(parsed.Fragment) != "" {
		return "", fmt.Errorf("fragment is not allowed")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func cmdCloudServe() {
	runtimeCfg := cloud.ConfigFromEnv()
	if err := validateCloudServeAuthConfig(); err != nil {
		fatal(err)
		return
	}
	runtime, err := newCloudRuntime(runtimeCfg)
	if err != nil {
		fatal(err)
		return
	}
	fmt.Printf("Starting Engram cloud server on port %d\n", runtimeCfg.Port)
	if err := runtime.Start(); err != nil {
		fatal(err)
	}
}

func validateCloudServeAuthConfig() error {
	token := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_TOKEN"))
	adminToken := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_ADMIN"))
	insecureNoAuth := envBool("ENGRAM_CLOUD_INSECURE_NO_AUTH")
	allowlist := normalizeAllowedProjects(cloud.ConfigFromEnv().AllowedProjects)
	jwtSecretEnv := strings.TrimSpace(os.Getenv("ENGRAM_JWT_SECRET"))
	if insecureNoAuth && token != "" {
		return fmt.Errorf("conflicting cloud auth configuration: ENGRAM_CLOUD_INSECURE_NO_AUTH=1 cannot be used together with ENGRAM_CLOUD_TOKEN")
	}
	if token != "" && len(allowlist) > 0 {
		if jwtSecretEnv == "" {
			return fmt.Errorf("authenticated cloud serve requires explicit ENGRAM_JWT_SECRET (non-default); refusing implicit default secret")
		}
		if cloud.IsDefaultJWTSecret(jwtSecretEnv) {
			return fmt.Errorf("authenticated cloud serve requires a non-default ENGRAM_JWT_SECRET; refusing development default")
		}
		return nil
	}
	if insecureNoAuth {
		if len(allowlist) == 0 {
			return fmt.Errorf("cloud project allowlist is required even in insecure mode: set ENGRAM_CLOUD_ALLOWED_PROJECTS to one or more project names")
		}
		if adminToken != "" {
			return fmt.Errorf("ENGRAM_CLOUD_ADMIN is not supported when ENGRAM_CLOUD_INSECURE_NO_AUTH=1; remove ENGRAM_CLOUD_ADMIN or enable authenticated mode")
		}
		fmt.Fprintln(os.Stderr, "warning: ENGRAM_CLOUD_INSECURE_NO_AUTH=1 disables cloud API authentication; do not use in production")
		return nil
	}
	if token == "" {
		return fmt.Errorf("cloud auth token is required: set ENGRAM_CLOUD_TOKEN (or ENGRAM_CLOUD_INSECURE_NO_AUTH=1 for local insecure development)")
	}
	return fmt.Errorf("cloud project allowlist is required: set ENGRAM_CLOUD_ALLOWED_PROJECTS to one or more project names")
}

func normalizeAllowedProjects(projects []string) []string {
	normalized := make([]string, 0, len(projects))
	seen := make(map[string]struct{})
	for _, project := range projects {
		name, _ := store.NormalizeProject(project)
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}

func cloudConfigPath(cfg store.Config) string {
	return filepath.Join(cfg.DataDir, "cloud.json")
}

func loadCloudConfig(cfg store.Config) (*cloudConfig, error) {
	path := cloudConfigPath(cfg)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cc cloudConfig
	if err := json.Unmarshal(b, &cc); err != nil {
		return nil, err
	}
	return &cc, nil
}

func saveCloudConfig(cfg store.Config, cc *cloudConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cloudConfigPath(cfg), b, 0o600)
}
