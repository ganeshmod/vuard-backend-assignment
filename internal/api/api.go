package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/vaurd/order-service/internal/models"
	"github.com/vaurd/order-service/internal/store"
)

// Server exposes the List Orders HTTP API.
type Server struct {
	store *store.Store
	mux   *http.ServeMux
}

// New creates an HTTP API server.
func New(st *store.Store) *Server {
	s := &Server{store: st, mux: http.NewServeMux()}
	s.routes()
	return s
}

// Handler returns the root HTTP handler.
func (s *Server) Handler() http.Handler {
	return withCORS(withLogging(s.mux))
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /{$}", s.handleRoot)
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /orders", s.handleListOrders)
	s.mux.HandleFunc("GET /api/v1/orders", s.handleListOrders)
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/orders?limit=20", http.StatusFound)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type listResponse struct {
	Orders []models.Order `json:"orders"`
	Total  int            `json:"total"`
	Limit  int            `json:"limit"`
	Offset int            `json:"offset"`
}

func (s *Server) handleListOrders(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	filter := store.ListFilter{
		Limit:  50,
		Offset: 0,
		SortAsc: false,
	}

	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		filter.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "offset must be a non-negative integer")
			return
		}
		filter.Offset = n
	}
	if v := q.Get("status"); v != "" {
		st := models.OrderStatus(v)
		if !models.ValidStatuses[st] {
			writeError(w, http.StatusBadRequest, "status must be one of: Received, Preparing, Complete, Cancelled")
			return
		}
		filter.Status = &st
	}
	switch q.Get("sort") {
	case "", "updated_at_desc", "updatedAtDesc":
		filter.SortAsc = false
	case "updated_at_asc", "updatedAtAsc":
		filter.SortAsc = true
	default:
		writeError(w, http.StatusBadRequest, "sort must be updated_at_desc or updated_at_asc")
		return
	}

	orders, total, err := s.store.ListOrders(r.Context(), filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list orders")
		return
	}

	writeJSON(w, http.StatusOK, listResponse{
		Orders: orders,
		Total:  total,
		Limit:  filter.Limit,
		Offset: filter.Offset,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rw, r)
		_ = start
		_ = rw
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
