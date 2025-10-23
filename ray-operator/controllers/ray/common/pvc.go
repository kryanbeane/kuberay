package common

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// BuildJITCheckpointPVC creates a PVC for JIT checkpoint storage
func BuildJITCheckpointPVC(instance *rayv1.RayCluster) (*corev1.PersistentVolumeClaim, error) {
	pvcName := utils.GetJITCheckpointPVCName(instance.Name, instance.Annotations)
	pvcSize := utils.GetJITCheckpointPVCSize(instance.Annotations)
	storageClass := utils.GetJITCheckpointStorageClass(instance.Annotations)

	quantity, err := resource.ParseQuantity(pvcSize)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PVC size %s: %w", pvcSize, err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: instance.Namespace,
			Labels: map[string]string{
				utils.RayClusterLabelKey:                instance.Name,
				utils.KubernetesApplicationNameLabelKey: utils.ApplicationName,
				utils.KubernetesCreatedByLabelKey:       utils.ComponentName,
				"ray.io/jit-checkpoint-pvc":             "true",
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce, // TEMP: ReadWriteOnce for Kind testing
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: quantity,
				},
			},
			StorageClassName: storageClass,
		},
	}

	return pvc, nil
}

// GetJITCheckpointVolumeMount returns the volume mount for JIT checkpoint storage
func GetJITCheckpointVolumeMount(instance *rayv1.RayCluster) corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      "jit-checkpoint-storage",
		MountPath: utils.DefaultJITCheckpointMountPath,
	}
}

// GetJITCheckpointVolume returns the volume for JIT checkpoint storage
// Note: pvcBaseName should be the RayJob name if owned by a RayJob, otherwise RayCluster name
func GetJITCheckpointVolume(pvcBaseName string, annotations map[string]string) corev1.Volume {
	pvcName := utils.GetJITCheckpointPVCName(pvcBaseName, annotations)
	return corev1.Volume{
		Name: "jit-checkpoint-storage",
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	}
}
