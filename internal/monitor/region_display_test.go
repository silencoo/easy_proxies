package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDisplayRegion(t *testing.T) {
	tests := []struct {
		name     string
		snapshot Snapshot
		want     string
	}{
		{name: "reported geoip wins", snapshot: Snapshot{NodeInfo: NodeInfo{Name: "日本节点", Region: "de"}}, want: "de"},
		{name: "resident takes precedence", snapshot: Snapshot{NodeInfo: NodeInfo{Name: "香港 residential home"}}, want: "resident"},
		{name: "hong kong", snapshot: Snapshot{NodeInfo: NodeInfo{Name: "[ikuuu]🇭🇰 香港Z05 | IEPL"}}, want: "hk"},
		{name: "japan", snapshot: Snapshot{NodeInfo: NodeInfo{Name: "日本标准 IEPL 专线"}}, want: "jp"},
		{name: "korea", snapshot: Snapshot{NodeInfo: NodeInfo{Tag: "KR-Seoul"}}, want: "kr"},
		{name: "united states", snapshot: Snapshot{NodeInfo: NodeInfo{Name: "US Los Angeles"}}, want: "us"},
		{name: "taiwan", snapshot: Snapshot{NodeInfo: NodeInfo{Name: "🇹🇼 台湾Z01"}}, want: "tw"},
		{name: "singapore", snapshot: Snapshot{NodeInfo: NodeInfo{Country: "Singapore"}}, want: "sg"},
		{name: "token does not match word fragment", snapshot: Snapshot{NodeInfo: NodeInfo{Name: "homework server"}}, want: "other"},
		{name: "unknown", snapshot: Snapshot{NodeInfo: NodeInfo{Name: "premium node", Region: "unknown"}}, want: "other"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := displayRegion(test.snapshot); got != test.want {
				t.Fatalf("displayRegion() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestHandleNodesUsesDisplayRegions(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer manager.Stop()

	handle := manager.Register(NodeInfo{Tag: "hk-1", Name: "[ikuuu]🇭🇰 香港Z05 | IEPL", Region: "other"})
	handle.MarkInitialCheckDone(true)
	server := &Server{mgr: manager}
	recorder := httptest.NewRecorder()
	server.handleNodes(recorder, httptest.NewRequest(http.MethodGet, "/api/nodes", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Nodes       []Snapshot     `json:"nodes"`
		RegionStats map[string]int `json:"region_stats"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.RegionStats["hk"] != 1 {
		t.Fatalf("region_stats = %#v, want hk=1", payload.RegionStats)
	}
	if len(payload.Nodes) != 1 || payload.Nodes[0].Region != "hk" {
		t.Fatalf("nodes = %#v, want one node with region hk", payload.Nodes)
	}
}
