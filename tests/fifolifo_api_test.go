package tests

import (
	"bytes"
	"encoding/json"
	"hydrakv/server"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFifoLifoAPI(t *testing.T) {
	s := server.NewServer(0, "127.0.0.1")
	handler := s.Handler()

	dbName := "APITESTDB"
	s.NewDB(dbName)
	dbPrefix := "/db/" + dbName

	t.Run("Create FiFoLiFo - Success", func(t *testing.T) {
		payload := server.NewLiFoFifo{
			Name:  "testqueue",
			Limit: 10,
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, dbPrefix+"/fifolifo", bytes.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("Expected status 201, got %d", w.Code)
		}
	})

	t.Run("Create FiFoLiFo - Validation Fail (Invalid Name)", func(t *testing.T) {
		payload := server.NewLiFoFifo{
			Name:  "invalid name!", // alphanum required
			Limit: 10,
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, dbPrefix+"/fifolifo", bytes.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for invalid name, got %d", w.Code)
		}
	})

	t.Run("Create FiFoLiFo - Validation Fail (Invalid Limit)", func(t *testing.T) {
		payload := server.NewLiFoFifo{
			Name:  "validname",
			Limit: 0, // min=1 required
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, dbPrefix+"/fifolifo", bytes.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for limit 0, got %d", w.Code)
		}
	})

	t.Run("Push and Pop Operations", func(t *testing.T) {
		queueName := "opsqueue"
		// Create
		payloadCreate := server.NewLiFoFifo{Name: queueName, Limit: 5}
		bodyCreate, _ := json.Marshal(payloadCreate)
		reqCreate := httptest.NewRequest(http.MethodPost, dbPrefix+"/fifolifo", bytes.NewReader(bodyCreate))
		handler.ServeHTTP(httptest.NewRecorder(), reqCreate)

		// Push "first"
		payloadPush1 := server.PushFiFoLiFo{Name: queueName, Value: "first"}
		bodyPush1, _ := json.Marshal(payloadPush1)
		reqPush1 := httptest.NewRequest(http.MethodPut, dbPrefix+"/fifolifo", bytes.NewReader(bodyPush1))
		wPush1 := httptest.NewRecorder()
		handler.ServeHTTP(wPush1, reqPush1)
		if wPush1.Code != http.StatusOK {
			t.Errorf("Push 1 failed: %d", wPush1.Code)
		}

		// Push "second"
		payloadPush2 := server.PushFiFoLiFo{Name: queueName, Value: "second"}
		bodyPush2, _ := json.Marshal(payloadPush2)
		reqPush2 := httptest.NewRequest(http.MethodPut, dbPrefix+"/fifolifo", bytes.NewReader(bodyPush2))
		handler.ServeHTTP(httptest.NewRecorder(), reqPush2)

		// FIFO Pop (should be "first")
		payloadPop := server.PopFiFoLiFo{Name: queueName}
		bodyPop, _ := json.Marshal(payloadPop)
		reqPopF := httptest.NewRequest(http.MethodPost, dbPrefix+"/fifo", bytes.NewReader(bodyPop))
		wPopF := httptest.NewRecorder()
		handler.ServeHTTP(wPopF, reqPopF)
		if wPopF.Code != http.StatusOK {
			t.Errorf("FIFO Pop failed: %d", wPopF.Code)
		}
		var valF string
		json.NewDecoder(wPopF.Body).Decode(&valF)
		if valF != "first" {
			t.Errorf("Expected 'first', got '%s'", valF)
		}

		// Push "third"
		payloadPush3 := server.PushFiFoLiFo{Name: queueName, Value: "third"}
		bodyPush3, _ := json.Marshal(payloadPush3)
		reqPush3 := httptest.NewRequest(http.MethodPut, dbPrefix+"/fifolifo", bytes.NewReader(bodyPush3))
		handler.ServeHTTP(httptest.NewRecorder(), reqPush3)

		// LIFO Pop (should be "third")
		reqPopL := httptest.NewRequest(http.MethodPost, dbPrefix+"/lifo", bytes.NewReader(bodyPop))
		wPopL := httptest.NewRecorder()
		handler.ServeHTTP(wPopL, reqPopL)
		if wPopL.Code != http.StatusOK {
			t.Errorf("LIFO Pop failed: %d", wPopL.Code)
		}
		var valL string
		json.NewDecoder(wPopL.Body).Decode(&valL)
		if valL != "third" {
			t.Errorf("Expected 'third', got '%s'", valL)
		}
	})

	t.Run("Push Validation - Empty Value", func(t *testing.T) {
		payload := server.PushFiFoLiFo{
			Name:  "testqueue",
			Value: "", // required, min=1
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPut, dbPrefix+"/fifolifo", bytes.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for empty value push, got %d", w.Code)
		}
	})

	t.Run("Delete FiFoLiFo", func(t *testing.T) {
		name := "deletequeue"
		// Create first
		pC := server.NewLiFoFifo{Name: name, Limit: 1}
		bC, _ := json.Marshal(pC)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, dbPrefix+"/fifolifo", bytes.NewReader(bC)))

		// Delete
		payload := server.DeleteFiFoLiFo{Name: name}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodDelete, dbPrefix+"/fifolifo", bytes.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Delete failed: %d", w.Code)
		}

		// Verify deleted (Push should fail)
		reqPush := httptest.NewRequest(http.MethodPut, dbPrefix+"/fifolifo", bytes.NewReader(body)) // same payload structure works for name
		wPush := httptest.NewRecorder()
		handler.ServeHTTP(wPush, reqPush)
		if wPush.Code == http.StatusOK {
			t.Errorf("Push should have failed for deleted queue")
		}
	})
	t.Run("Pop Validation - Invalid Name", func(t *testing.T) {
		payload := server.PopFiFoLiFo{
			Name: "invalid name!",
		}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, dbPrefix+"/fifo", bytes.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for invalid name in pop, got %d", w.Code)
		}
	})

	t.Run("Pop from Empty Queue", func(t *testing.T) {
		name := "emptyqueue"
		pC := server.NewLiFoFifo{Name: name, Limit: 10}
		bC, _ := json.Marshal(pC)
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, dbPrefix+"/fifolifo", bytes.NewReader(bC)))

		payload := server.PopFiFoLiFo{Name: name}
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(http.MethodPost, dbPrefix+"/fifo", bytes.NewReader(body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 500 for pop from empty queue, got %d", w.Code)
		}
	})
}
