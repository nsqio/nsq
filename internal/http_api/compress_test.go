package http_api

import (
    "net/http/httptest"
    "testing"
    "net/http"
)


// Test generated using Keploy
func TestCompressResponseWriterHeader(t *testing.T) {
    mockResponseWriter := httptest.NewRecorder()
    compressWriter := &compressResponseWriter{
        ResponseWriter: mockResponseWriter,
    }

    compressWriter.Header().Set("Content-Type", "application/json")
    if compressWriter.Header().Get("Content-Type") != "application/json" {
        t.Errorf("Expected Content-Type to be 'application/json', got %v", compressWriter.Header().Get("Content-Type"))
    }
}

// Test generated using Keploy
func TestCompressResponseWriterWriteHeader(t *testing.T) {
    mockResponseWriter := httptest.NewRecorder()
    compressWriter := &compressResponseWriter{
        ResponseWriter: mockResponseWriter,
    }

    compressWriter.Header().Set("Content-Length", "123")
    compressWriter.WriteHeader(http.StatusOK)

    if compressWriter.Header().Get("Content-Length") != "" {
        t.Errorf("Expected Content-Length to be removed, but it was not")
    }
    if mockResponseWriter.Code != http.StatusOK {
        t.Errorf("Expected status code to be %v, got %v", http.StatusOK, mockResponseWriter.Code)
    }
}


// Test generated using Keploy
func TestCompressResponseWriterWrite(t *testing.T) {
    mockResponseWriter := httptest.NewRecorder()
    compressWriter := &compressResponseWriter{
        ResponseWriter: mockResponseWriter,
        Writer:         mockResponseWriter,
    }

    data := []byte("test data")
    _, err := compressWriter.Write(data)
    if err != nil {
        t.Fatalf("Unexpected error: %v", err)
    }

    if compressWriter.Header().Get("Content-Type") == "" {
        t.Errorf("Expected Content-Type to be set, but it was not")
    }
    if compressWriter.Header().Get("Content-Length") != "" {
        t.Errorf("Expected Content-Length to be removed, but it was not")
    }
}


// Test generated using Keploy
func TestCompressHandlerGzip(t *testing.T) {
    handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Write([]byte("test data"))
    })
    compressHandler := CompressHandler(handler)

    req := httptest.NewRequest("GET", "/", nil)
    req.Header.Set("Accept-Encoding", "gzip")
    rec := httptest.NewRecorder()

    compressHandler.ServeHTTP(rec, req)

    if rec.Header().Get("Content-Encoding") != "gzip" {
        t.Errorf("Expected Content-Encoding to be 'gzip', got %v", rec.Header().Get("Content-Encoding"))
    }
}


// Test generated using Keploy
func TestCompressHandlerDeflate(t *testing.T) {
    handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Write([]byte("test data"))
    })
    compressHandler := CompressHandler(handler)

    req := httptest.NewRequest("GET", "/", nil)
    req.Header.Set("Accept-Encoding", "deflate")
    rec := httptest.NewRecorder()

    compressHandler.ServeHTTP(rec, req)

    if rec.Header().Get("Content-Encoding") != "deflate" {
        t.Errorf("Expected Content-Encoding to be 'deflate', got %v", rec.Header().Get("Content-Encoding"))
    }
}

