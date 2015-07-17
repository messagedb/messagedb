package controllers_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/messagedb/messagedb/services/httpd"
	"github.com/messagedb/messagedb/services/httpd/controllers"
)

func TestPing(t *testing.T) {
	router := httpd.NewRouter()
	config := httpd.Config{BindAddress: "127.0.0.1:0"}
	_ = controllers.NewPingController(router, config.LogEnabled, config.WriteTracing)

	res := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "http://example.com/ping", nil)

	router.ServeHTTP(res, req)

	expect(t, res.Code, http.StatusOK)
	expect(t, res.Body.String(), "{\"result\":\"pong\"}\n")
}
