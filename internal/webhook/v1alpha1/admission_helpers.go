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

package v1alpha1

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// DefaultResource wraps webhook defaulting handlers with shared type assertion and logging logic.
func DefaultResource[T client.Object](obj runtime.Object, kind string, log logr.Logger, fn func(T) error) error {
	typed, err := typeAssert[T](obj, kind)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("Defaulting %s", kind), "name", typed.GetName())
	if fn != nil {
		return fn(typed)
	}
	return nil
}

// ValidateCreateResource wraps webhook create validators with consistent type assertion and logging.
func ValidateCreateResource[T client.Object](ctx context.Context, obj runtime.Object, kind string, log logr.Logger, fn func(context.Context, T) error) (admission.Warnings, error) {
	typed, err := typeAssert[T](obj, kind)
	if err != nil {
		return nil, err
	}
	log.Info(fmt.Sprintf("Validation for %s upon creation", kind), "name", typed.GetName())
	if fn != nil {
		if err := fn(ctx, typed); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// ValidateDeleteResource wraps webhook delete validators with consistent type assertion and logging.
func ValidateDeleteResource[T client.Object](obj runtime.Object, kind string, log logr.Logger, fn func(T) error) (admission.Warnings, error) {
	typed, err := typeAssert[T](obj, kind)
	if err != nil {
		return nil, err
	}
	log.Info(fmt.Sprintf("Validation for %s upon deletion", kind), "name", typed.GetName())
	if fn != nil {
		if err := fn(typed); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func typeAssert[T client.Object](obj runtime.Object, kind string) (T, error) {
	typed, ok := obj.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("expected a %s object but got %T", kind, obj)
	}
	return typed, nil
}
