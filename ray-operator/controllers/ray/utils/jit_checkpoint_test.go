package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsJITCheckpointEnabled(t *testing.T) {
	tests := []struct {
		annotations map[string]string
		name        string
		expected    bool
	}{
		{
			name:        "enabled with lowercase true",
			annotations: map[string]string{RayJITCheckpointEnabledAnnotationKey: "true"},
			expected:    true,
		},
		{
			name:        "enabled with uppercase TRUE",
			annotations: map[string]string{RayJITCheckpointEnabledAnnotationKey: "TRUE"},
			expected:    true,
		},
		{
			name:        "disabled with false",
			annotations: map[string]string{RayJITCheckpointEnabledAnnotationKey: "false"},
			expected:    false,
		},
		{
			name:        "not set",
			annotations: map[string]string{},
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsJITCheckpointEnabled(tt.annotations))
		})
	}
}

func TestGetJITCheckpointKillWait(t *testing.T) {
	tests := []struct {
		annotations map[string]string
		name        string
		expected    float64
	}{
		{
			name:        "custom value",
			annotations: map[string]string{RayJITCheckpointKillWaitAnnotationKey: "5.0"},
			expected:    5.0,
		},
		{
			name:        "default value",
			annotations: map[string]string{},
			expected:    DefaultJITCheckpointKillWait,
		},
		{
			name:        "invalid value uses default",
			annotations: map[string]string{RayJITCheckpointKillWaitAnnotationKey: "invalid"},
			expected:    DefaultJITCheckpointKillWait,
		},
		{
			name:        "negative value uses default",
			annotations: map[string]string{RayJITCheckpointKillWaitAnnotationKey: "-1.0"},
			expected:    DefaultJITCheckpointKillWait,
		},
		{
			name:        "zero value uses default",
			annotations: map[string]string{RayJITCheckpointKillWaitAnnotationKey: "0"},
			expected:    DefaultJITCheckpointKillWait,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetJITCheckpointKillWait(tt.annotations))
		})
	}
}

func TestGetJITCheckpointPVCName(t *testing.T) {
	tests := []struct {
		name        string
		clusterName string
		annotations map[string]string
		expected    string
	}{
		{
			name:        "custom PVC name",
			clusterName: "test-cluster",
			annotations: map[string]string{RayJITCheckpointPVCNameAnnotationKey: "my-pvc"},
			expected:    "my-pvc",
		},
		{
			name:        "generated PVC name",
			clusterName: "test-cluster",
			annotations: map[string]string{},
			expected:    "test-cluster-jit-checkpoints",
		},
		{
			name:        "empty custom name uses generated",
			clusterName: "my-cluster",
			annotations: map[string]string{RayJITCheckpointPVCNameAnnotationKey: ""},
			expected:    "my-cluster-jit-checkpoints",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetJITCheckpointPVCName(tt.clusterName, tt.annotations))
		})
	}
}

func TestGetJITCheckpointPVCSize(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		expected    string
	}{
		{
			name:        "custom size",
			annotations: map[string]string{RayJITCheckpointPVCSizeAnnotationKey: "50Gi"},
			expected:    "50Gi",
		},
		{
			name:        "default size",
			annotations: map[string]string{},
			expected:    DefaultJITCheckpointPVCSize,
		},
		{
			name:        "empty custom size uses default",
			annotations: map[string]string{RayJITCheckpointPVCSizeAnnotationKey: ""},
			expected:    DefaultJITCheckpointPVCSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetJITCheckpointPVCSize(tt.annotations))
		})
	}
}

func TestGetJITCheckpointStorageClass(t *testing.T) {
	tests := []struct {
		annotations map[string]string
		expected    *string
		name        string
	}{
		{
			name:        "custom storage class",
			annotations: map[string]string{RayJITCheckpointStorageClassAnnotationKey: "fast-ssd"},
			expected:    stringPtr("fast-ssd"),
		},
		{
			name:        "no storage class (cluster default)",
			annotations: map[string]string{},
			expected:    nil,
		},
		{
			name:        "empty storage class uses cluster default",
			annotations: map[string]string{RayJITCheckpointStorageClassAnnotationKey: ""},
			expected:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetJITCheckpointStorageClass(tt.annotations)
			if tt.expected == nil {
				assert.Nil(t, result)
			} else {
				assert.NotNil(t, result)
				assert.Equal(t, *tt.expected, *result)
			}
		})
	}
}

func TestCalculateJITCheckpointGracePeriod(t *testing.T) {
	tests := []struct {
		name     string
		killWait float64
		expected int64
	}{
		{
			name:     "default kill_wait",
			killWait: 3.0,
			expected: 33, // 3 + 30 buffer
		},
		{
			name:     "custom kill_wait",
			killWait: 10.0,
			expected: 40, // 10 + 30 buffer
		},
		{
			name:     "small kill_wait",
			killWait: 1.0,
			expected: 31, // 1 + 30 buffer
		},
		{
			name:     "large kill_wait",
			killWait: 60.0,
			expected: 90, // 60 + 30 buffer
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, CalculateJITCheckpointGracePeriod(tt.killWait))
		})
	}
}

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}
