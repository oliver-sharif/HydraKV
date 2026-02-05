package tests

import (
	"encoding/json"
	"hydrakv/envhandler"
	serverpkg "hydrakv/server"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAPIKeyEngine(t *testing.T) {
	// 1. API Key Engine per Env aktivieren
	oldVal := *envhandler.ENV.APIKEY_ENABLED
	*envhandler.ENV.APIKEY_ENABLED = true
	defer func() {
		*envhandler.ENV.APIKEY_ENABLED = oldVal
	}()

	// 2. Server starten
	s := serverpkg.NewServer(0, "127.0.0.1")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()
	client := ts.Client()
	base := ts.URL

	dbName := "keytestdb"

	// 3. Datenbank erstellen (Post an /create ist public)
	resp, body := doJSON(t, client, http.MethodPost, base+"/create", serverpkg.NewDB{Name: dbName})
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("Konnte Test-DB nicht erstellen: %d %s", resp.StatusCode, string(body))
	}

	var created serverpkg.NewDBCreated
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatalf("Fehler beim Dekodieren der Antwort: %v", err)
	}

	apiKey := created.ApiKey
	if apiKey == "" {
		t.Fatal("Kein API-Key von der API erhalten")
	}

	// 4. Test: Zugriff OHNE API-Key (Sollte 401 Unauthorized liefern)
	resp, _ = doJSON(t, client, http.MethodGet, base+"/db/"+dbName, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Erwartete 401 bei fehlendem API-Key, got %d", resp.StatusCode)
	}

	// 5. Test: Zugriff mit FALSCHEM API-Key (Sollte 401 Unauthorized liefern)
	req, _ := http.NewRequest(http.MethodGet, base+"/db/"+dbName, nil)
	req.Header.Set("X-API-Key", "wrong-key")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Request fehlgeschlagen: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("Erwartete 401 bei falschem API-Key, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// 6. Test: Zugriff mit KORREKTEM API-Key (Sollte 200 OK liefern)
	req, _ = http.NewRequest(http.MethodGet, base+"/db/"+dbName, nil)
	req.Header.Set("X-API-Key", apiKey)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("Request mit korrektem Key fehlgeschlagen: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Erwartete 200 bei korrektem API-Key, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}
