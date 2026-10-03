package llm

import "testing"

func TestParseSparkResponse(t *testing.T) {
	tests := []struct {
		name       string
		data       map[string]interface{}
		wantStatus float64
		wantText   string
		wantErr    bool
	}{
		{
			name: "normal text chunk",
			data: map[string]interface{}{
				"header":  map[string]interface{}{"code": float64(0)},
				"payload": map[string]interface{}{"choices": map[string]interface{}{"status": float64(1), "text": []interface{}{map[string]interface{}{"content": "hello"}}}},
			},
			wantStatus: 1,
			wantText:   "hello",
		},
		{
			name: "empty final frame",
			data: map[string]interface{}{
				"header":  map[string]interface{}{"code": float64(0)},
				"payload": map[string]interface{}{"choices": map[string]interface{}{"status": float64(2), "text": []interface{}{map[string]interface{}{}}}},
			},
			wantStatus: 2,
		},
		{
			name: "malformed payload",
			data: map[string]interface{}{
				"header": map[string]interface{}{"code": float64(0)},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status, text, err := parseSparkResponse(tt.data)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseSparkResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if status != tt.wantStatus || text != tt.wantText {
				t.Fatalf("parseSparkResponse() = (%v, %q), want (%v, %q)", status, text, tt.wantStatus, tt.wantText)
			}
		})
	}
}
