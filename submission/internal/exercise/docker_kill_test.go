package exercise

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFaultControllerChecksKillResponse(t *testing.T) {
	for _, status := range []int{204, 404, 500} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" || r.URL.Path != "/containers/phi-messaging-app1-1/kill" || r.URL.Query().Get("signal") != "SIGKILL" {
				t.Error("unexpected fault operation")
			}
			w.WriteHeader(status)
		}))
		err := requestApp1Kill(context.Background(), srv.Client(), srv.URL)
		srv.Close()
		if (err == nil) != (status == 204) {
			t.Fatal("fault request failure was hidden", status, err)
		}
	}
}
