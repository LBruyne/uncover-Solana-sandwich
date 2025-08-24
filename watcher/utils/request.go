package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	DefaultRetryTimes    = 5
	DefaultRetryInterval = 500 * time.Millisecond
	DefaultTimeout       = 10 * time.Second
)

func GetUrlResponse(url string, params map[string]string, result any, logger *slog.Logger) error {
	return GetUrlResponseWithRetry(url, params, result, 1, logger)
}

func GetUrlResponseWithRetry(url string, params map[string]string, result any, retry int, logger *slog.Logger) error {
	reqUrl := url
	if len(params) > 0 {
		q := "?"
		for k, v := range params {
			q += fmt.Sprintf("%s=%s&", k, v)
		}
		reqUrl += q[:len(q)-1] // Remove trailing '&'
	}

	var lastErr error
	for i := 0; i < retry; i++ {
		lastErr = doGet(reqUrl, result, logger)
		if lastErr == nil {
			return nil
		}
		logger.Warn("GET request failed, retrying...", "url", reqUrl, "attempt", i+1, "err", lastErr)
		time.Sleep(DefaultRetryInterval)
	}
	return fmt.Errorf("GET request failed after %d attempts: %w", retry, lastErr)
}

func PostUrlResponse(url string, body any, result any, logger *slog.Logger) error {
	return PostUrlResponseWithRetry(url, body, result, 1, logger)
}

func PostUrlResponseWithRetry(url string, body any, result any, retry int, logger *slog.Logger) error {
	var lastErr error
	for i := 0; i < retry; i++ {
		lastErr = doPost(url, body, result, logger)
		if lastErr == nil {
			return nil
		}
		logger.Warn("POST request failed, retrying...", "url", url, "attempt", i+1, "err", lastErr)
		time.Sleep(DefaultRetryInterval)
	}
	return fmt.Errorf("POST request failed after %d attempts: %w", retry, lastErr)
}

func doGet(url string, result any, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Error("Failed to create GET request", "url", url, "err", err)
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error("GET request error", "url", url, "err", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logger.Error("GET request non-200 response", "url", url, "status", resp.StatusCode, "body", string(bodyBytes))
		return fmt.Errorf("GET request returned status %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("Failed to read GET response body", "url", url, "err", err)
		return err
	}

	if err := json.Unmarshal(bodyBytes, result); err != nil {
		logger.Error("Failed to unmarshal GET response", "url", url, "err", err, "body", string(bodyBytes))
		return err
	}

	return nil
}

func doPost(url string, body any, result any, logger *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultTimeout)
	defer cancel()

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		logger.Error("Failed to marshal POST body", "url", url, "err", err, "body", body)
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		logger.Error("Failed to create POST request", "url", url, "err", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error("POST request error", "url", url, "err", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyResp, _ := io.ReadAll(resp.Body)
		logger.Error("POST request non-200 response", "url", url, "status", resp.StatusCode, "body", string(bodyResp))
		return fmt.Errorf("POST request returned status %d", resp.StatusCode)
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Error("Failed to read POST response body", "url", url, "err", err)
		return err
	}

	if err := json.Unmarshal(respBytes, result); err != nil {
		logger.Error("Failed to unmarshal POST response", "url", url, "err", err, "body", string(respBytes))
		return err
	}

	return nil
}
