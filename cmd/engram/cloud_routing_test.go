package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/Gentleman-Programming/engram/internal/cloud/autosync"
	"github.com/Gentleman-Programming/engram/internal/cloud/constants"
	"github.com/Gentleman-Programming/engram/internal/store"
)

func TestCloudConfigMigrateLegacyCreatesDefaultRemote(t *testing.T) {
	cc := &cloudConfig{ServerURL: "https://legacy.example.test", Token: "legacy-token"}
	cc.migrateLegacy()

	if cc.ServerURL != "" || cc.Token != "" {
		t.Fatalf("expected legacy fields cleared after migration, got %+v", cc)
	}
	rc, ok := cc.Remotes[legacyDefaultRemoteName]
	if !ok {
		t.Fatalf("expected default remote to be created, got remotes=%+v", cc.Remotes)
	}
	if rc.ServerURL != "https://legacy.example.test" || rc.Token != "legacy-token" {
		t.Fatalf("default remote mismatch: %+v", rc)
	}
	if cc.DefaultRemote != legacyDefaultRemoteName {
		t.Fatalf("expected default_remote=%q, got %q", legacyDefaultRemoteName, cc.DefaultRemote)
	}
}

func TestCloudConfigMigrateLegacyNoopWhenEmpty(t *testing.T) {
	cc := &cloudConfig{}
	cc.migrateLegacy()
	if len(cc.Remotes) != 0 || cc.DefaultRemote != "" {
		t.Fatalf("expected no-op migration for empty config, got %+v", cc)
	}
}

func TestResolveForProjectExplicitRouteWins(t *testing.T) {
	cc := &cloudConfig{
		Remotes: map[string]remoteConfig{
			"personal": {ServerURL: "https://personal.test", Token: "p"},
			"team":     {ServerURL: "https://team.test", Token: "t"},
		},
		Routes:        map[string]string{"acme": "team"},
		DefaultRemote: "personal",
	}
	eff, name, reason, err := cc.resolveForProject("acme")
	if err != nil || reason != "" {
		t.Fatalf("unexpected resolution error: reason=%q err=%v", reason, err)
	}
	if name != "team" || eff.ServerURL != "https://team.test" || eff.Token != "t" {
		t.Fatalf("expected team remote, got name=%q eff=%+v", name, eff)
	}
}

func TestResolveForProjectFallsBackToDefault(t *testing.T) {
	cc := &cloudConfig{
		Remotes:       map[string]remoteConfig{"personal": {ServerURL: "https://personal.test", Token: "p"}},
		DefaultRemote: "personal",
	}
	eff, name, reason, err := cc.resolveForProject("finhub")
	if err != nil || reason != "" {
		t.Fatalf("unexpected resolution error: reason=%q err=%v", reason, err)
	}
	if name != "personal" || eff.ServerURL != "https://personal.test" {
		t.Fatalf("expected personal default remote, got name=%q eff=%+v", name, eff)
	}
}

func TestResolveForProjectNoRouteNoDefaultIsDenied(t *testing.T) {
	cc := &cloudConfig{
		Remotes: map[string]remoteConfig{"team": {ServerURL: "https://team.test", Token: "t"}},
		Routes:  map[string]string{"acme": "team"},
		// no DefaultRemote → strict default-deny
	}
	_, _, reason, err := cc.resolveForProject("client-a")
	if err == nil {
		t.Fatal("expected default-deny error for an unrouted project")
	}
	if reason != constants.ReasonBlockedNoRoute {
		t.Fatalf("expected reason %q, got %q", constants.ReasonBlockedNoRoute, reason)
	}
}

func TestResolveForProjectEmptyConfigIsConfigError(t *testing.T) {
	cc := &cloudConfig{}
	_, _, reason, err := cc.resolveForProject("anything")
	if err == nil {
		t.Fatal("expected config error when cloud is unconfigured")
	}
	if reason != constants.ReasonCloudConfigError {
		t.Fatalf("expected reason %q, got %q", constants.ReasonCloudConfigError, reason)
	}
}

func TestResolveForProjectUnknownRemoteIsConfigError(t *testing.T) {
	cc := &cloudConfig{
		Remotes: map[string]remoteConfig{"personal": {ServerURL: "https://personal.test"}},
		Routes:  map[string]string{"acme": "ghost"},
	}
	_, name, reason, err := cc.resolveForProject("acme")
	if err == nil {
		t.Fatal("expected config error when a route points to an unknown remote")
	}
	if reason != constants.ReasonCloudConfigError {
		t.Fatalf("expected reason %q, got %q", constants.ReasonCloudConfigError, reason)
	}
	if name != "ghost" {
		t.Fatalf("expected the offending remote name surfaced, got %q", name)
	}
}

func TestApplyEnvOverridesFoldsIntoDefaultRemote(t *testing.T) {
	t.Setenv("ENGRAM_CLOUD_SERVER", "https://env.test")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "env-token")

	cc := &cloudConfig{}
	cc.applyEnvOverrides()

	if cc.DefaultRemote != legacyDefaultRemoteName {
		t.Fatalf("expected env overrides to create a default remote, got %q", cc.DefaultRemote)
	}
	rc := cc.Remotes[legacyDefaultRemoteName]
	if rc.ServerURL != "https://env.test" || rc.Token != "env-token" {
		t.Fatalf("env override mismatch: %+v", rc)
	}
}

func TestNormalizedForRuntimeDoesNotMutateReceiver(t *testing.T) {
	t.Setenv("ENGRAM_CLOUD_SERVER", "")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "")

	cc := &cloudConfig{ServerURL: "https://legacy.test", Token: "tok"}
	rt := cc.normalizedForRuntime()

	if cc.ServerURL != "https://legacy.test" || cc.Token != "tok" || len(cc.Remotes) != 0 {
		t.Fatalf("normalizedForRuntime mutated the receiver: %+v", cc)
	}
	if rt.Remotes[legacyDefaultRemoteName].ServerURL != "https://legacy.test" {
		t.Fatalf("expected runtime view to carry the migrated default remote, got %+v", rt)
	}
}

// TestSyncCloudDefaultDenyForUnroutedProject exercises the full default-deny
// path: a remote exists but the enrolled project has no route and there is no
// default remote, so sync must fail loudly with the blocked_no_route reason
// code and persist it to sync state.
func TestSyncCloudDefaultDenyForUnroutedProject(t *testing.T) {
	stubExitWithPanic(t)
	workDir := t.TempDir()
	withCwd(t, workDir)

	cfg := testConfig(t)
	t.Setenv("ENGRAM_CLOUD_SERVER", "")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "")

	if err := saveCloudConfig(cfg, &cloudConfig{
		Remotes: map[string]remoteConfig{
			"team": {ServerURL: "https://team.example.test", Token: "team-token"},
		},
	}); err != nil {
		t.Fatalf("save cloud config: %v", err)
	}

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := s.EnrollProject("client-a"); err != nil {
		_ = s.Close()
		t.Fatalf("enroll project: %v", err)
	}
	_ = s.Close()

	withArgs(t, "engram", "sync", "--cloud", "--project", "client-a")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdSync(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected fatal exit for unrouted project, got %v", recovered)
	}
	if !strings.Contains(stderr, constants.ReasonBlockedNoRoute) {
		t.Fatalf("expected blocked_no_route surfaced, got %q", stderr)
	}

	s2, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer s2.Close()
	state, err := s2.GetSyncState(cloudTargetKeyForProject("client-a"))
	if err != nil {
		t.Fatalf("get sync state: %v", err)
	}
	if state.ReasonCode == nil || *state.ReasonCode != constants.ReasonBlockedNoRoute {
		t.Fatalf("expected blocked_no_route reason code in sync state, got %v", state.ReasonCode)
	}
}

// TestCloudRemoteAndRouteCLILifecycle exercises the full `remote add` ->
// `remote list` -> `route set` -> `route list` -> orphaned-route protection ->
// `route unset` -> `remote remove` lifecycle end to end via the CLI dispatch
// entrypoints, asserting on stdout/stderr content and exit behavior.
func TestCloudRemoteAndRouteCLILifecycle(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	t.Setenv("ENGRAM_CLOUD_SERVER", "")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "")

	const secretToken = "super-secret-remote-token"

	steps := []struct {
		name          string
		args          []string
		expectExit    bool
		stdoutMustHave []string
		stdoutMustNot  []string
		stderrMustHave []string
	}{
		{
			name:          "remote add creates the remote",
			args:          []string{"engram", "cloud", "remote", "add", "team", "--server", "https://team.example.test", "--token", secretToken},
			stdoutMustHave: []string{"team", "https://team.example.test"},
		},
		{
			name:          "remote list shows the remote but never the token value",
			args:          []string{"engram", "cloud", "remote", "list"},
			stdoutMustHave: []string{"team", "https://team.example.test", "token set"},
			stdoutMustNot:  []string{secretToken},
		},
		{
			name: "route set routes a project to the remote",
			args: []string{"engram", "cloud", "route", "set", "--project", "acme", "--remote", "team"},
			stdoutMustHave: []string{"acme", "team"},
		},
		{
			name:          "route list shows the project route",
			args:          []string{"engram", "cloud", "route", "list"},
			stdoutMustHave: []string{"acme", "team"},
		},
		{
			name:           "remote remove refuses to orphan a routed project",
			args:           []string{"engram", "cloud", "remote", "remove", "team"},
			expectExit:     true,
			stderrMustHave: []string{"still routed", "acme"},
		},
		{
			name:          "route unset clears the project route",
			args:          []string{"engram", "cloud", "route", "unset", "--project", "acme"},
			stdoutMustHave: []string{"acme"},
		},
		{
			name:          "remote remove now succeeds",
			args:          []string{"engram", "cloud", "remote", "remove", "team"},
			stdoutMustHave: []string{"team"},
		},
	}

	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			withArgs(t, step.args...)
			stdout, stderr, recovered := captureOutputAndRecover(t, func() { cmdCloud(cfg) })
			_, exited := recovered.(exitCode)
			if exited != step.expectExit {
				t.Fatalf("expected exitCode=%v, got recovered=%v stdout=%q stderr=%q", step.expectExit, recovered, stdout, stderr)
			}
			for _, want := range step.stdoutMustHave {
				if !strings.Contains(stdout, want) {
					t.Fatalf("expected stdout to contain %q, got %q", want, stdout)
				}
			}
			for _, unwanted := range step.stdoutMustNot {
				if strings.Contains(stdout, unwanted) {
					t.Fatalf("expected stdout to NOT contain %q, got %q", unwanted, stdout)
				}
			}
			for _, want := range step.stderrMustHave {
				if !strings.Contains(stderr, want) {
					t.Fatalf("expected stderr to contain %q, got %q", want, stderr)
				}
			}
		})
	}
}

// TestCloudConfigEnvTokenNeverPersistedToDisk asserts that ENGRAM_CLOUD_TOKEN
// is used at runtime but never written to cloud.json on disk, regardless of
// which config-mutating subcommand runs.
func TestCloudConfigEnvTokenNeverPersistedToDisk(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	const envToken = "super-secret-env-token-not-on-disk"
	t.Setenv("ENGRAM_CLOUD_TOKEN", envToken)
	t.Setenv("ENGRAM_CLOUD_SERVER", "")

	withArgs(t, "engram", "cloud", "remote", "add", "team", "--server", "https://team.example.test")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCloud(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("remote add failed: panic=%v stderr=%q", recovered, stderr)
	}

	raw, err := os.ReadFile(cloudConfigPath(cfg))
	if err != nil {
		t.Fatalf("read cloud.json: %v", err)
	}
	if strings.Contains(string(raw), envToken) {
		t.Fatalf("ENGRAM_CLOUD_TOKEN must never be persisted to cloud.json, got %s", raw)
	}
}

// TestCmdCloudConfigServerUpdatesActiveDefaultRemoteAfterMigration is the C2
// regression test: once a `default` remote exists (created implicitly by
// `remote add` migrating the legacy shape), a subsequent `cloud config
// --server` must still update the *effective* destination — not silently be
// ignored because migrateLegacy's `if !exists` guard already created the
// remote from the first --server call.
func TestCmdCloudConfigServerUpdatesActiveDefaultRemoteAfterMigration(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	t.Setenv("ENGRAM_CLOUD_SERVER", "")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "")

	withArgs(t, "engram", "cloud", "config", "--server", "https://url1.example.test")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCloud(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("first `cloud config --server` failed: panic=%v stderr=%q", recovered, stderr)
	}

	withArgs(t, "engram", "cloud", "remote", "add", "x", "--server", "https://x.example.test")
	_, stderr, recovered = captureOutputAndRecover(t, func() { cmdCloud(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("remote add failed: panic=%v stderr=%q", recovered, stderr)
	}

	withArgs(t, "engram", "cloud", "config", "--server", "https://url2.example.test")
	_, stderr, recovered = captureOutputAndRecover(t, func() { cmdCloud(cfg) })
	if recovered != nil || stderr != "" {
		t.Fatalf("second `cloud config --server` failed: panic=%v stderr=%q", recovered, stderr)
	}

	eff, remoteName, reason, err := resolveRoutedCloudConfig(cfg, "any-project")
	if err != nil {
		t.Fatalf("resolveRoutedCloudConfig: reason=%q err=%v", reason, err)
	}
	if eff.ServerURL != "https://url2.example.test" {
		t.Fatalf("expected effective default server to be the second `cloud config --server` URL, got %q (remote=%q)", eff.ServerURL, remoteName)
	}

	// The legacy field must also reflect the latest --server call, since an
	// existing test (TestCmdCloudConfigAcceptsValidServerURL) reads it directly.
	cc, err := loadCloudConfig(cfg)
	if err != nil {
		t.Fatalf("loadCloudConfig: %v", err)
	}
	if cc.ServerURL != "https://url2.example.test" {
		t.Fatalf("expected legacy ServerURL field to also be updated, got %q", cc.ServerURL)
	}
}

// TestApplyEnvOverridesDoesNotDefeatStrictDenyRouting is the C3 regression
// test: a strict-deny config (remotes + routes configured, no DefaultRemote)
// must not have env vars manufacture a default remote — an unrouted project
// must still resolve to blocked_no_route rather than syncing to the
// env-provided server.
func TestApplyEnvOverridesDoesNotDefeatStrictDenyRouting(t *testing.T) {
	t.Setenv("ENGRAM_CLOUD_SERVER", "https://env.example.test")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "env-token")

	cc := &cloudConfig{
		Remotes: map[string]remoteConfig{"team": {ServerURL: "https://team.test", Token: "team-tok"}},
		Routes:  map[string]string{"acme": "team"},
		// DefaultRemote intentionally empty: strict default-deny.
	}
	rt := cc.normalizedForRuntime()
	if rt.DefaultRemote != "" {
		t.Fatalf("env vars must not manufacture a default remote in a strict-deny config, got %q", rt.DefaultRemote)
	}
	if len(rt.Remotes) != 1 {
		t.Fatalf("expected no new remote to be created from env, got remotes=%+v", rt.Remotes)
	}

	_, _, reason, err := rt.resolveForProject("unrouted-client")
	if err == nil {
		t.Fatal("expected an unrouted project to still be denied even with env vars set")
	}
	if reason != constants.ReasonBlockedNoRoute {
		t.Fatalf("expected reason %q, got %q", constants.ReasonBlockedNoRoute, reason)
	}
}

// TestSyncCloudEnvOverridesDoNotBypassStrictDenyEndToEnd is the full
// integration form of the C3 fix: with a persisted strict-deny cloud.json and
// ENGRAM_CLOUD_SERVER/TOKEN set in the environment, an unrouted project must
// still be blocked_no_route rather than silently syncing to the env server.
func TestSyncCloudEnvOverridesDoNotBypassStrictDenyEndToEnd(t *testing.T) {
	stubExitWithPanic(t)
	workDir := t.TempDir()
	withCwd(t, workDir)

	cfg := testConfig(t)
	if err := saveCloudConfig(cfg, &cloudConfig{
		Remotes: map[string]remoteConfig{
			"team": {ServerURL: "https://team.example.test", Token: "team-token"},
		},
		Routes: map[string]string{"acme": "team"},
		// no DefaultRemote → strict default-deny
	}); err != nil {
		t.Fatalf("save cloud config: %v", err)
	}
	t.Setenv("ENGRAM_CLOUD_SERVER", "https://env-leak.example.test")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "env-leak-token")

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	if err := s.EnrollProject("client-b"); err != nil {
		_ = s.Close()
		t.Fatalf("enroll project: %v", err)
	}
	_ = s.Close()

	withArgs(t, "engram", "sync", "--cloud", "--project", "client-b")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdSync(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected fatal exit for unrouted project even with env vars set, got %v", recovered)
	}
	if !strings.Contains(stderr, constants.ReasonBlockedNoRoute) {
		t.Fatalf("expected blocked_no_route surfaced (env must not manufacture a default), got %q", stderr)
	}
	if strings.Contains(stderr, "env-leak.example.test") {
		t.Fatalf("must never reference the env-provided server for a denied project, got %q", stderr)
	}
}

// TestTryStartAutosyncGatesOnPerProjectRouting is the C1b safe-gate test:
// the single-transport autosync Manager has no per-project remote allowlist,
// so when an enrolled project is routed to a remote other than the default,
// autosync must refuse to start rather than risk pushing that project's
// mutations to the wrong destination.
func TestTryStartAutosyncGatesOnPerProjectRouting(t *testing.T) {
	cfg := testConfig(t)
	t.Setenv("ENGRAM_CLOUD_AUTOSYNC", "1")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "")
	t.Setenv("ENGRAM_CLOUD_SERVER", "")

	if err := saveCloudConfig(cfg, &cloudConfig{
		Remotes: map[string]remoteConfig{
			"default": {ServerURL: "https://default.example.test", Token: "default-tok"},
			"team":    {ServerURL: "https://team.example.test", Token: "team-tok"},
		},
		Routes:        map[string]string{"acme": "team"},
		DefaultRemote: "default",
	}); err != nil {
		t.Fatalf("save cloud config: %v", err)
	}

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer s.Close()
	if err := s.EnrollProject("acme"); err != nil {
		t.Fatalf("enroll acme: %v", err)
	}
	if err := s.EnrollProject("finhub"); err != nil {
		t.Fatalf("enroll finhub: %v", err)
	}

	managerCreated := false
	old := newAutosyncManager
	newAutosyncManager = func(_ *store.Store, _ autosync.CloudTransport, _ autosync.Config) startableAutosyncManager {
		managerCreated = true
		return &fakeStartableManager{}
	}
	defer func() { newAutosyncManager = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr, stopFn := tryStartAutosync(ctx, s, cfg)
	if mgr != nil || stopFn != nil {
		t.Fatalf("expected autosync to be disabled when an enrolled project (acme) is routed to a non-default remote, got mgr=%v stopFnIsNil=%v", mgr, stopFn == nil)
	}
	if managerCreated {
		t.Fatal("newAutosyncManager must not be called when per-project routing blocks autosync")
	}
}

// TestCmdCloudConfigRefusedInStrictDenyMode is the round-2 CRITICAL regression
// test: a strict-deny config (a named remote + a route, no DefaultRemote) must
// not have `engram cloud config --server` manufacture a `default` remote and
// silently become the new default. That would reopen the default-deny bypass
// (same bug class as C3): an unrouted project would stop being denied.
func TestCmdCloudConfigRefusedInStrictDenyMode(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)
	t.Setenv("ENGRAM_CLOUD_SERVER", "")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "")

	if err := saveCloudConfig(cfg, &cloudConfig{
		Remotes: map[string]remoteConfig{
			"team": {ServerURL: "https://team.example.test", Token: "team-token"},
		},
		Routes: map[string]string{"acme": "team"},
		// no DefaultRemote → strict default-deny
	}); err != nil {
		t.Fatalf("save cloud config: %v", err)
	}

	withArgs(t, "engram", "cloud", "config", "--server", "https://sneaky.example.test")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCloud(cfg) })
	if _, ok := recovered.(exitCode); !ok {
		t.Fatalf("expected fatal exit when strict-deny config has remotes but no default, got recovered=%v stderr=%q", recovered, stderr)
	}
	lowerStderr := strings.ToLower(stderr)
	if !strings.Contains(lowerStderr, "default") || !strings.Contains(lowerStderr, "remote") {
		t.Fatalf("expected error mentioning default/remote, got stderr=%q", stderr)
	}

	cc, err := loadCloudConfig(cfg)
	if err != nil {
		t.Fatalf("loadCloudConfig: %v", err)
	}
	if _, ok := cc.Remotes[legacyDefaultRemoteName]; ok {
		t.Fatalf("expected no `default` remote to be manufactured on disk, got remotes=%+v", cc.Remotes)
	}
	if strings.TrimSpace(cc.DefaultRemote) != "" {
		t.Fatalf("expected DefaultRemote to remain unset, got %q", cc.DefaultRemote)
	}
	if cc.ServerURL != "" {
		t.Fatalf("expected legacy ServerURL to remain unset (config must not be saved), got %q", cc.ServerURL)
	}

	rt := cc.normalizedForRuntime()
	_, _, reason, resolveErr := rt.resolveForProject("unrouted")
	if resolveErr == nil {
		t.Fatal("expected an unrouted project to still be denied after the refused config attempt")
	}
	if reason != constants.ReasonBlockedNoRoute {
		t.Fatalf("expected reason %q for unrouted project, got %q (err=%v)", constants.ReasonBlockedNoRoute, reason, resolveErr)
	}
}

// TestCloudSyncEnabledSurfacesUnknownRemote is the FIX 3 regression test: a
// project routed to a remote name that no longer exists in cc.Remotes must
// surface a cloud_config_error with the resolver's actual diagnostic message,
// not the generic "cloud_not_configured" message that would hide the cause.
func TestCloudSyncEnabledSurfacesUnknownRemote(t *testing.T) {
	cfg := testConfig(t)
	t.Setenv("ENGRAM_CLOUD_SERVER", "")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "")

	if err := saveCloudConfig(cfg, &cloudConfig{
		Remotes: map[string]remoteConfig{
			"personal": {ServerURL: "https://personal.example.test"},
		},
		Routes: map[string]string{"acme": "ghost"},
	}); err != nil {
		t.Fatalf("save cloud config: %v", err)
	}

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer s.Close()
	if err := s.EnrollProject("acme"); err != nil {
		t.Fatalf("enroll acme: %v", err)
	}

	provider := storeSyncStatusProvider{store: s, defaultProject: "acme", cfg: cfg}
	enabled, code, message := provider.cloudSyncEnabled("acme")
	if enabled {
		t.Fatalf("expected cloud sync disabled for a route to an unknown remote, got enabled=true")
	}
	if code != constants.ReasonCloudConfigError {
		t.Fatalf("expected reason code %q, got %q (message=%q)", constants.ReasonCloudConfigError, code, message)
	}
	if code == "cloud_not_configured" {
		t.Fatalf("must not collapse unknown-remote route into the generic cloud_not_configured reason")
	}
	if !strings.Contains(message, "ghost") {
		t.Fatalf("expected message to name the offending remote %q, got %q", "ghost", message)
	}
}

// TestResolveForProjectEmptyProjectHasCleanMessage is the FIX 4 regression
// test: an empty project name must not produce a message embedding an empty
// quoted name (`project "" has no cloud route...`); it should read as a clean
// "no project specified" message instead, while still using the
// blocked_no_route reason code.
func TestResolveForProjectEmptyProjectHasCleanMessage(t *testing.T) {
	cc := &cloudConfig{
		Remotes: map[string]remoteConfig{"team": {ServerURL: "https://team.test", Token: "t"}},
		// no DefaultRemote → default-deny applies even to an empty project name
	}
	_, _, reason, err := cc.resolveForProject("")
	if err == nil {
		t.Fatal("expected an error for an empty project with no default remote")
	}
	if reason != constants.ReasonBlockedNoRoute {
		t.Fatalf("expected reason %q, got %q", constants.ReasonBlockedNoRoute, reason)
	}
	if strings.Contains(err.Error(), `""`) {
		t.Fatalf("expected a clean message without an embedded empty-quoted project name, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), "no project specified") {
		t.Fatalf("expected message to mention no project specified, got %q", err.Error())
	}
}

// TestCloudUpgradeRollbackWritesConfigWithSecretPermissions is the FIX 2
// regression test: the cloud.json restored by `cloud upgrade rollback` can
// contain remote tokens, so it must be written with the same restrictive
// permissions as saveCloudConfig (0600), not world/group-readable 0644.
func TestCloudUpgradeRollbackWritesConfigWithSecretPermissions(t *testing.T) {
	stubExitWithPanic(t)
	cfg := testConfig(t)

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	snapshotJSON := `{"remotes":{"team":{"server_url":"https://team.example.test","token":"super-secret-rollback-token"}},"default_remote":"team"}`
	if err := s.SaveCloudUpgradeState(store.CloudUpgradeState{
		Project: "acme",
		Stage:   store.UpgradeStagePlanned,
		Snapshot: store.CloudUpgradeSnapshot{
			CloudConfigPresent: true,
			CloudConfigJSON:    snapshotJSON,
		},
	}); err != nil {
		s.Close()
		t.Fatalf("seed upgrade state: %v", err)
	}
	s.Close()

	withArgs(t, "engram", "cloud", "upgrade", "rollback", "--project", "acme")
	_, stderr, recovered := captureOutputAndRecover(t, func() { cmdCloudUpgrade(cfg) })
	if recovered != nil {
		t.Fatalf("rollback failed: panic=%v stderr=%q", recovered, stderr)
	}

	info, err := os.Stat(cloudConfigPath(cfg))
	if err != nil {
		t.Fatalf("stat cloud.json: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("expected cloud.json to be written with mode 0600, got %o", perm)
	}
}

// TestTryStartAutosyncRunsInPureSingleRemoteMode asserts the C1b safe gate
// does not regress the historical single-remote/legacy autosync behavior:
// when every enrolled project routes to the default remote (or has no
// explicit route at all), autosync must still start normally.
func TestTryStartAutosyncRunsInPureSingleRemoteMode(t *testing.T) {
	cfg := testConfig(t)
	t.Setenv("ENGRAM_CLOUD_AUTOSYNC", "1")
	t.Setenv("ENGRAM_CLOUD_TOKEN", "")
	t.Setenv("ENGRAM_CLOUD_SERVER", "")

	if err := saveCloudConfig(cfg, &cloudConfig{
		ServerURL: "https://legacy.example.test",
		Token:     "legacy-tok",
	}); err != nil {
		t.Fatalf("save cloud config: %v", err)
	}

	s, err := store.New(cfg)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer s.Close()
	if err := s.EnrollProject("acme"); err != nil {
		t.Fatalf("enroll acme: %v", err)
	}

	managerCreated := false
	old := newAutosyncManager
	newAutosyncManager = func(_ *store.Store, _ autosync.CloudTransport, _ autosync.Config) startableAutosyncManager {
		managerCreated = true
		return &fakeStartableManager{}
	}
	defer func() { newAutosyncManager = old }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr, stopFn := tryStartAutosync(ctx, s, cfg)
	if mgr == nil || stopFn == nil {
		t.Fatalf("expected autosync to start in pure single-remote/legacy mode, got mgr=%v stopFnIsNil=%v", mgr, stopFn == nil)
	}
	if !managerCreated {
		t.Fatal("expected newAutosyncManager to be called in pure single-remote/legacy mode")
	}
	stopFn()
}
