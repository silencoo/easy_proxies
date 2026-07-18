package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"easy_proxies/internal/config"
)

type nodeConfigManagerStub struct {
	nodes       []config.NodeConfig
	createCalls int
}

func (s *nodeConfigManagerStub) ListConfigNodes(context.Context) ([]config.NodeConfig, error) {
	return append([]config.NodeConfig(nil), s.nodes...), nil
}
func (s *nodeConfigManagerStub) CreateNode(_ context.Context, node config.NodeConfig) (config.NodeConfig, error) {
	s.createCalls++
	return node, nil
}
func (s *nodeConfigManagerStub) UpdateNode(context.Context, string, config.NodeConfig) (config.NodeConfig, error) {
	return config.NodeConfig{}, errors.New("not implemented")
}
func (s *nodeConfigManagerStub) DeleteNode(context.Context, string) error {
	return errors.New("not implemented")
}
func (s *nodeConfigManagerStub) TriggerReload(context.Context) error { return nil }
func (s *nodeConfigManagerStub) ConfigSnapshot() (*config.Config, uint64) {
	return &config.Config{}, 0
}
func (s *nodeConfigManagerStub) CommitConfig(context.Context, uint64, *config.Config, func(*config.Config) (func() error, error)) error {
	return nil
}

func TestConfigNodesExposeStableIDsForDuplicateNames(t *testing.T) {
	stub := &nodeConfigManagerStub{nodes: []config.NodeConfig{
		{Name: "duplicate", URI: "socks5://127.0.0.1:1081#first"},
		{Name: "duplicate", URI: "socks5://127.0.0.1:1082#second"},
	}}
	server := &Server{nodeMgr: stub}
	recorder := httptest.NewRecorder()

	server.handleConfigNodes(recorder, httptest.NewRequest(http.MethodGet, "/api/nodes/config", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Nodes []nodeConfigResponse `json:"nodes"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Nodes) != 2 || payload.Nodes[0].ID == "" || payload.Nodes[0].ID == payload.Nodes[1].ID {
		t.Fatalf("stable IDs missing or ambiguous: %#v", payload.Nodes)
	}
}

func TestConfigNodeListMasksSecretsUntilExplicitItemRead(t *testing.T) {
	node := config.NodeConfig{
		Name: "private", URI: "vless://uuid:password@example.com:443/private?token=secret",
		Username: "listener-user", Password: "listener-secret",
	}
	stub := &nodeConfigManagerStub{nodes: []config.NodeConfig{node}}
	server := &Server{nodeMgr: stub}

	listRecorder := httptest.NewRecorder()
	server.handleConfigNodes(listRecorder, httptest.NewRequest(http.MethodGet, "/api/nodes/config", nil))
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	if body := listRecorder.Body.String(); strings.Contains(body, "uuid") || strings.Contains(body, "password") || strings.Contains(body, "secret") || strings.Contains(body, "listener-user") {
		t.Fatalf("list response exposed node credentials: %s", body)
	}

	itemRecorder := httptest.NewRecorder()
	path := "/api/nodes/config/" + node.NodeKey()
	server.handleConfigNodeItem(itemRecorder, httptest.NewRequest(http.MethodGet, path, nil))
	if itemRecorder.Code != http.StatusOK {
		t.Fatalf("item status=%d body=%s", itemRecorder.Code, itemRecorder.Body.String())
	}
	var payload struct {
		Node nodeConfigResponse `json:"node"`
	}
	if err := json.Unmarshal(itemRecorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Node.URI != node.URI {
		t.Fatalf("explicit item URI=%q, want %q", payload.Node.URI, node.URI)
	}
	if payload.Node.Username != node.Username || payload.Node.Password != node.Password {
		t.Fatalf("explicit item credentials=(%q,%q), want (%q,%q)", payload.Node.Username, payload.Node.Password, node.Username, node.Password)
	}
}

func TestConfigNodeWritesUseStrictBoundedJSON(t *testing.T) {
	for _, test := range []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "unknown field", body: `{"name":"node","uri":"socks5://127.0.0.1:1080","extra":true}`, wantStatus: http.StatusBadRequest},
		{name: "trailing JSON", body: `{"name":"node","uri":"socks5://127.0.0.1:1080"}{}`, wantStatus: http.StatusBadRequest},
		{name: "oversized", body: strings.Repeat(" ", int(maxNodeConfigBodyBytes)+1), wantStatus: http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			stub := &nodeConfigManagerStub{}
			server := &Server{nodeMgr: stub}
			recorder := httptest.NewRecorder()
			server.handleConfigNodes(recorder, httptest.NewRequest(http.MethodPost, "/api/nodes/config", strings.NewReader(test.body)))
			if recorder.Code != test.wantStatus || stub.createCalls != 0 {
				t.Fatalf("status=%d calls=%d body=%s", recorder.Code, stub.createCalls, recorder.Body.String())
			}
		})
	}
}
