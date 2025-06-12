package sybase

import (
	"encoding/json"
	"errors"
	"fmt"
)

func (s *Sybase) Raw(sql string) (*RawResponse, error) {
	if !s.IsConnected() {
		return nil, errors.New("database isn't connected")
	}

	s.mu.Lock()
	s.queryCount++
	msgID := s.queryCount

	respChan := make(chan QueryResponse, 1)
	s.currentQueries[msgID] = respChan
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.currentQueries, msgID)
		s.mu.Unlock()
	}()

	req := QueryRequest{
		MsgID:       msgID,
		TransID:     -1,
		FinishTrans: true,
		SQL:         sql,
	}

	reqBytes, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("error marshaling query: %w", err)
	}

	// aplica la query directamente
	if _, err := fmt.Fprintf(s.stdin, "%s\n", reqBytes); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}

	if s.logs {
		fmt.Println("Full JSON being sent: ")
	}

	resp := <-respChan

	if len(resp.Result) == 0 && resp.Error != "" {
		return nil, errors.New(resp.Error)
	}

	response, err := convertToRawResponse(resp.Result)

	if err != nil {
		return nil, err
	}

	return response, nil
}
