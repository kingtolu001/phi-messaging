package exercise

import (
	"context"
	"fmt"
	"net"
	"net/http"
)

// Only the fault-test container mounts this socket. Application containers do not.
// Its fallback permits one operation: killing this project's app1 container.
func killApp1(ctx context.Context, socket string) error {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	return requestApp1Kill(ctx, &http.Client{Transport: transport}, "http://docker")
}

func requestApp1Kill(ctx context.Context, client *http.Client, base string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/containers/phi-messaging-app1-1/kill?signal=SIGKILL", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("Docker app1 kill returned HTTP %d", response.StatusCode)
	}
	return nil
}
