/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestMergeStringMaps(t *testing.T) {
	out := MergeStringMaps(
		map[string]string{"a": "1", "b": "1"},
		nil,
		map[string]string{"b": "2", "c": "3"},
	)
	require.Equal(t, map[string]string{"a": "1", "b": "2", "c": "3"}, out)

	require.Nil(t, MergeStringMaps(nil, map[string]string{}))
}

func TestCloneServicePorts(t *testing.T) {
	in := []corev1.ServicePort{{
		Name:     "grpc",
		Protocol: corev1.ProtocolTCP,
		Port:     443,
	}}
	out := CloneServicePorts(in)
	require.Equal(t, in, out)
	require.NotSame(t, &in[0], &out[0])

	require.Nil(t, CloneServicePorts(nil))
}

func TestEnsureServiceCreateAndUpdate(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	owner := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "owner",
			Namespace: "demo",
			UID:       types.UID("owner-uid"),
		},
	}

	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-svc",
			Namespace: "demo",
			Labels:    map[string]string{"app": "demo"},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "demo"},
			Ports: []corev1.ServicePort{{
				Name: "grpc",
				Port: 8080,
			}},
		},
	}

	ctx := context.Background()
	created, result, err := EnsureService(ctx, client, scheme, owner, desired)
	require.NoError(t, err)
	require.Equal(t, controllerutil.OperationResultCreated, result)
	require.Equal(t, desired.Name, created.Name)

	// Simulate the API server allocating immutable fields.
	var stored corev1.Service
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &stored))
	stored.Spec.ClusterIP = "10.0.0.1"
	stored.Spec.ClusterIPs = []string{"10.0.0.1"}
	require.NoError(t, client.Update(ctx, &stored))

	desiredUpdate := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        desired.Name,
			Namespace:   desired.Namespace,
			Annotations: map[string]string{"patched": "true"},
		},
		Spec: corev1.ServiceSpec{
			Selector: desired.Spec.Selector,
			Ports: []corev1.ServicePort{{
				Name: "grpc",
				Port: 9090,
			}},
		},
	}

	updated, updateResult, err := EnsureService(ctx, client, scheme, owner, desiredUpdate)
	require.NoError(t, err)
	require.Equal(t, controllerutil.OperationResultUpdated, updateResult)
	require.Equal(t, "10.0.0.1", updated.Spec.ClusterIP, "immutable ClusterIP should be preserved")
	require.Equal(t, int32(9090), updated.Spec.Ports[0].Port)
	require.Equal(t, "true", updated.Annotations["patched"])
}
