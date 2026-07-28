package tests

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	k6cloud "github.com/grafana/k6-cloud-openapi-client-go/k6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	provtest "go.k6.io/k6/v2/internal/cloudapi/provisioning/test"
	v6 "go.k6.io/k6/v2/internal/cloudapi/v6"
	"go.k6.io/k6/v2/internal/cmd"
	"go.k6.io/k6/v2/lib/fsext"
)

// TestExtProvMatrix is a behaviour matrix for how the cloud metrics-push
// credentials (MetricsPushURL / TestRunToken) are resolved across the
// self-provisioned, externally-provisioned and legacy paths.
//
// It is intentionally written against only public/stable symbols so the
// exact same file can be dropped onto master, the current (#6170) branch,
// and the fixed branch, producing a pass/fail grid. The key observable is
// which bearer token k6 actually pushes metrics with — captured by the mock
// server — using distinct sentinel tokens per source.
//
// Group A rows are regression / no-breaking-change guards (must pass on
// master AND the fix). Group B rows guard that the externally-provisioned
// feature still works.
const (
	epOrgToken    = "org-longlived-token" // K6_CLOUD_TOKEN: legacy / relay push
	epScopedToken = "test-run-token-abc"  // provtest.DefaultStartLocalExecutionResponse
	epBogusToken  = "bogus-env-token"     // stray K6_CLOUD_TEST_RUN_TOKEN in the env
	epExtToken    = "ext-scoped-token"    // externally-supplied scoped token
	epStaleCfgTok = "stale-config-token"  // stale value living in a k6 config file
)

const epScript = `
export const options = { cloud: { name: 'matrix', projectID: 123456 } };
export default function () {};`

// reqRecorder records the bearer token of every request the mock sees, so
// tests can assert exactly which token k6 used to push metrics.
type reqRecorder struct {
	mu   sync.Mutex
	reqs []recReq
}

type recReq struct{ path, token string }

func (r *reqRecorder) handler(w http.ResponseWriter, req *http.Request) {
	_, _ = io.Copy(io.Discard, req.Body)
	r.mu.Lock()
	r.reqs = append(r.reqs, recReq{req.URL.Path, epBearer(req)})
	r.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (r *reqRecorder) tokensForPath(sub string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, q := range r.reqs {
		if strings.Contains(q.path, sub) {
			out = append(out, q.token)
		}
	}
	return out
}

func (r *reqRecorder) allTokens() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.reqs))
	for _, q := range r.reqs {
		out = append(out, q.token)
	}
	return out
}

// epBearer extracts the credential from the Authorization header,
// tolerating both auth schemes k6 uses: "Bearer <tok>" for the
// provisioning/v6 push and "Token <tok>" for the legacy v1 push.
func epBearer(req *http.Request) string {
	auth := req.Header.Get("Authorization")
	auth = strings.TrimPrefix(auth, "Bearer ")
	auth = strings.TrimPrefix(auth, "Token ")
	return auth
}

func epFail(t *testing.T, msg string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		assert.Fail(t, msg)
		w.WriteHeader(http.StatusInternalServerError)
	})
}

// assertPushedWith asserts at least one request hit a path containing sub,
// and every such request carried the expected bearer token.
func assertPushedWith(t *testing.T, rec *reqRecorder, sub, token string) {
	t.Helper()
	toks := rec.tokensForPath(sub)
	require.NotEmptyf(t, toks, "expected a metrics push to a path containing %q", sub)
	for _, tk := range toks {
		assert.Equalf(t, token, tk, "push to %q used an unexpected bearer token", sub)
	}
}

func assertTokenNeverUsed(t *testing.T, rec *reqRecorder, token string) {
	t.Helper()
	assert.NotContainsf(t, rec.allTokens(), token, "token %q must never be sent", token)
}

func assertPathNeverHit(t *testing.T, rec *reqRecorder, sub string) {
	t.Helper()
	assert.Emptyf(t, rec.tokensForPath(sub), "no request should hit a path containing %q", sub)
}

// epSetupSelfProv wires a self-provisioned `k6 cloud run --local-execution`
// against a provisioning mock whose start_local_execution returns the scoped
// token epScopedToken and a push URL pointing back at the mock (/v1/metrics).
func epSetupSelfProv(t *testing.T) (*GlobalTestState, *provtest.Server, *reqRecorder, *atomic.Bool) {
	t.Helper()

	ts := makeTestState(t, epScript, []string{"--local-execution"})
	ts.Env["K6_CLOUD_TOKEN"] = epOrgToken

	srv := provtest.NewServer(t)
	rec := &reqRecorder{}
	var notified atomic.Bool

	srv.HandleCreateLoadTest(123456, func(w http.ResponseWriter, _ *http.Request) {
		res := k6cloud.NewLoadTestApiModelWithDefaults()
		res.SetId(provtest.DefaultLoadTestID)
		writeProvJSON(w, http.StatusCreated, res)
	})
	srv.HandleStartLocalExecution(provtest.DefaultLoadTestID, func(w http.ResponseWriter, _ *http.Request) {
		resp := provtest.DefaultStartLocalExecutionResponse()
		resp.SetArchiveUploadUrl(srv.PresignedUploadURL())
		resp.SetTestRunDetailsPageUrl(fmt.Sprintf("%s/runs/%d", srv.URL, provtest.DefaultTestRunID))
		rc := resp.GetRuntimeConfig()
		m := rc.GetMetrics()
		m.SetPushUrl(srv.URL + "/v1/metrics")
		rc.SetMetrics(m)
		resp.SetRuntimeConfig(rc)
		writeProvJSON(w, http.StatusOK, resp)
	})
	srv.HandlePresignedUpload(provtest.PresignedUploadPath, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv.HandleFetchTestRun(provtest.DefaultTestRunID, []v6.TestProgress{{Status: v6.StatusInitializing}})
	srv.HandleNotify(provtest.DefaultTestRunID, func(w http.ResponseWriter, _ *http.Request) {
		notified.Store(true)
		w.WriteHeader(http.StatusOK)
	})

	// Record the provisioning-mode push (/v1/metrics) and any stray push
	// (e.g. a clobbered URL landing on the catch-all).
	srv.Mux.HandleFunc("/v1/metrics", rec.handler)
	srv.Mux.HandleFunc("/", rec.handler)

	ts.Env["K6_CLOUD_HOST"] = srv.URL
	ts.Env["K6_CLOUD_HOST_V6"] = srv.URL
	return ts, srv, rec, &notified
}

func TestExtProvMatrix(t *testing.T) {
	t.Parallel()

	// ---- Group A: regression / no-breaking-change guards ----

	t.Run("A1_self_prov_happy_path", func(t *testing.T) {
		t.Parallel()
		ts, _, rec, notified := epSetupSelfProv(t)
		cmd.ExecuteWithGlobalState(ts.GlobalState)
		assertPushedWith(t, rec, "/v1/metrics", epScopedToken)
		assert.True(t, notified.Load(), "self-provisioned run must notify at end")
	})

	t.Run("A2_self_prov_stray_token_env", func(t *testing.T) {
		t.Parallel()
		ts, _, rec, notified := epSetupSelfProv(t)
		ts.Env["K6_CLOUD_TEST_RUN_TOKEN"] = epBogusToken
		cmd.ExecuteWithGlobalState(ts.GlobalState)
		assertPushedWith(t, rec, "/v1/metrics", epScopedToken)
		assertTokenNeverUsed(t, rec, epBogusToken)
		assert.True(t, notified.Load())
	})

	t.Run("A3_self_prov_stray_push_url_env", func(t *testing.T) {
		t.Parallel()
		ts, srv, rec, _ := epSetupSelfProv(t)
		ts.Env["K6_CLOUD_METRICS_PUSH_URL"] = srv.URL + "/bogus-metrics"
		cmd.ExecuteWithGlobalState(ts.GlobalState)
		assertPushedWith(t, rec, "/v1/metrics", epScopedToken)
		assertPathNeverHit(t, rec, "/bogus-metrics")
	})

	t.Run("A4_self_prov_both_stray_env", func(t *testing.T) {
		t.Parallel()
		ts, srv, rec, _ := epSetupSelfProv(t)
		ts.Env["K6_CLOUD_TEST_RUN_TOKEN"] = epBogusToken
		ts.Env["K6_CLOUD_METRICS_PUSH_URL"] = srv.URL + "/bogus-metrics"
		cmd.ExecuteWithGlobalState(ts.GlobalState)
		assertPushedWith(t, rec, "/v1/metrics", epScopedToken)
		assertTokenNeverUsed(t, rec, epBogusToken)
		assertPathNeverHit(t, rec, "/bogus-metrics")
	})

	t.Run("A5_out_cloud_stray_token_env", func(t *testing.T) {
		t.Parallel()
		ts := getSingleFileTestState(t, epScript, []string{"-v", "--log-output=stdout", "--out=cloud"}, 0)
		ts.Env["K6_CLOUD_TOKEN"] = epOrgToken
		ts.Env["K6_CLOUD_TEST_RUN_TOKEN"] = epBogusToken

		rec := &reqRecorder{}
		const refID = 1337
		srv := getTestServer(t, map[string]http.Handler{
			"POST ^/v1/tests$": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, `{"reference_id": "%d", "config": {}}`, refID)
			}),
			fmt.Sprintf("POST ^/v1/tests/%d$", refID): http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}),
			"POST ^/v2/metrics/": http.HandlerFunc(rec.handler),
		})
		t.Cleanup(srv.Close)
		ts.Env["K6_CLOUD_HOST"] = srv.URL

		cmd.ExecuteWithGlobalState(ts.GlobalState)
		assertPushedWith(t, rec, "/v2/metrics/", epOrgToken)
		assertTokenNeverUsed(t, rec, epBogusToken)
	})

	t.Run("A6_pushref_no_scoped_creds", func(t *testing.T) {
		t.Parallel()
		ts := makeTestState(t, epScript, []string{"--local-execution"})
		ts.Env["K6_CLOUD_TOKEN"] = epOrgToken
		ts.Env["K6_CLOUD_PUSH_REF_ID"] = "99999"

		rec := &reqRecorder{}
		srv := getTestServer(t, map[string]http.Handler{
			"POST ^/v1/tests$":        epFail(t, "CreateTestRun must not be called with PushRefID"),
			"POST ^/provisioning/v1/": epFail(t, "provisioning API must not be called with PushRefID"),
			"POST ^/cloud/v6/":        epFail(t, "v6 API must not be called with PushRefID"),
			"POST ^/v2/metrics/":      http.HandlerFunc(rec.handler),
		})
		t.Cleanup(srv.Close)
		ts.Env["K6_CLOUD_HOST"] = srv.URL
		ts.Env["K6_CLOUD_HOST_V6"] = srv.URL

		cmd.ExecuteWithGlobalState(ts.GlobalState)
		assertPushedWith(t, rec, "/v2/metrics/", epOrgToken)
	})

	t.Run("A7_out_cloud_pushref_one_scoped_env", func(t *testing.T) {
		t.Parallel()
		ts := getSingleFileTestState(t, epScript, []string{"-v", "--log-output=stdout", "--out=cloud"}, 0)
		ts.Env["K6_CLOUD_TOKEN"] = epOrgToken
		ts.Env["K6_CLOUD_PUSH_REF_ID"] = "1337"
		ts.Env["K6_CLOUD_TEST_RUN_TOKEN"] = epExtToken // single stray scoped cred

		rec := &reqRecorder{}
		srv := getTestServer(t, map[string]http.Handler{
			"POST ^/v1/tests$":   epFail(t, "CreateTestRun must not be called with PushRefID"),
			"POST ^/v2/metrics/": http.HandlerFunc(rec.handler),
		})
		t.Cleanup(srv.Close)
		ts.Env["K6_CLOUD_HOST"] = srv.URL

		cmd.ExecuteWithGlobalState(ts.GlobalState)
		assertPushedWith(t, rec, "/v2/metrics/", epOrgToken)
	})

	t.Run("A8_self_prov_stale_config_file", func(t *testing.T) {
		t.Parallel()
		ts, _, rec, _ := epSetupSelfProv(t)
		cfg := []byte(`{"collectors":{"cloud":{"testRunToken":"` + epStaleCfgTok + `"}}}`)
		require.NoError(t, ts.FS.MkdirAll(filepath.Dir(ts.Flags.ConfigFilePath), 0o755))
		require.NoError(t, fsext.WriteFile(ts.FS, ts.Flags.ConfigFilePath, cfg, 0o644))
		cmd.ExecuteWithGlobalState(ts.GlobalState)
		assertPushedWith(t, rec, "/v1/metrics", epScopedToken)
		assertTokenNeverUsed(t, rec, epStaleCfgTok)
	})

	// ---- Group B: externally-provisioned feature preservation ----

	t.Run("B1_pushref_both_scoped_creds", func(t *testing.T) {
		t.Parallel()
		ts := makeTestState(t, epScript, []string{"--local-execution"})
		ts.Env["K6_CLOUD_TOKEN"] = epOrgToken
		ts.Env["K6_CLOUD_PUSH_REF_ID"] = "99999"

		rec := &reqRecorder{}
		srv := getTestServer(t, map[string]http.Handler{
			"POST ^/provisioning/v1/": epFail(t, "provisioning API must not be called with PushRefID"),
			"POST ^/cloud/v6/":        epFail(t, "v6 API must not be called with PushRefID"),
			"POST ^/v1/metrics":       http.HandlerFunc(rec.handler),
			"POST ^/v2/metrics/":      http.HandlerFunc(rec.handler),
		})
		t.Cleanup(srv.Close)
		ts.Env["K6_CLOUD_HOST"] = srv.URL
		ts.Env["K6_CLOUD_HOST_V6"] = srv.URL
		ts.Env["K6_CLOUD_METRICS_PUSH_URL"] = srv.URL + "/v1/metrics"
		ts.Env["K6_CLOUD_TEST_RUN_TOKEN"] = epExtToken

		cmd.ExecuteWithGlobalState(ts.GlobalState)
		// The externally-provisioned run pushes with the env-supplied creds.
		assertPushedWith(t, rec, "/v1/metrics", epExtToken)
	})

	t.Run("B2_pushref_one_scoped_cred_errors", func(t *testing.T) {
		t.Parallel()
		ts := makeTestState(t, epScript, []string{"--local-execution", "--log-output=stdout"})
		ts.Env["K6_CLOUD_TOKEN"] = epOrgToken
		ts.Env["K6_CLOUD_PUSH_REF_ID"] = "99999"
		ts.Env["K6_CLOUD_TEST_RUN_TOKEN"] = epExtToken // only one of the pair
		ts.ExpectedExitCode = -1

		srv := getTestServer(t, map[string]http.Handler{})
		t.Cleanup(srv.Close)
		ts.Env["K6_CLOUD_HOST"] = srv.URL
		ts.Env["K6_CLOUD_HOST_V6"] = srv.URL

		cmd.ExecuteWithGlobalState(ts.GlobalState)
		out := ts.Stdout.String() + ts.Stderr.String()
		assert.Contains(t, out, "must be set together",
			"expected a clear both-or-neither error for partial scoped creds")
	})
}
