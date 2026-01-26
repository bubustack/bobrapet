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
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestDeleteIfExistsDeletesObject(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "default",
		},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc.DeepCopy()).Build()

	logger := testr.New(t)
	err := DeleteIfExists(context.Background(), client, svc, logger)
	require.NoError(t, err)

	var fetched corev1.Service
	getErr := client.Get(context.Background(), types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, &fetched)
	require.Error(t, getErr)
}

func TestDeleteIfExistsHandlesNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	err := DeleteIfExists(context.Background(), client, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "missing",
			Namespace: "default",
		},
	}, testr.New(t))
	require.NoError(t, err)
}

func TestDeleteIfExistsSkipsWhenDeleting(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	client := fake.NewClientBuilder().WithScheme(scheme).Build()

	now := metav1.NewTime(time.Now())
	obj := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "deleting",
			Namespace:         "default",
			DeletionTimestamp: &now,
		},
	}
	require.NoError(t, DeleteIfExists(context.Background(), client, obj, testr.New(t)))
}
