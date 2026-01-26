package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/bubustack/core/contracts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type kubeRef struct {
	Namespace string
	Name      string
	Key       string
	Format    string
}

func parseKubeRef(value any) (*kubeRef, error) {
	switch v := value.(type) {
	case string:
		return parseKubeRefString(v)
	case map[string]any:
		return parseKubeRefMap(v)
	default:
		return nil, fmt.Errorf("invalid ref type %T", value)
	}
}

func parseKubeRefString(raw string) (*kubeRef, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("reference is empty")
	}
	parts := strings.SplitN(raw, ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
		return nil, fmt.Errorf("invalid reference %q (expected name:key or namespace/name:key)", raw)
	}
	key := strings.TrimSpace(parts[1])
	nsName := strings.TrimSpace(parts[0])
	ns, name := splitNamespaceName(nsName)
	if name == "" {
		return nil, fmt.Errorf("invalid reference %q (missing name)", raw)
	}
	if ns == "" {
		ns = defaultNamespace()
	}
	if ns == "" {
		return nil, fmt.Errorf("namespace is required for reference %q", raw)
	}
	return &kubeRef{
		Namespace: ns,
		Name:      name,
		Key:       key,
		Format:    defaultRefFormat,
	}, nil
}

func parseKubeRefMap(raw map[string]any) (*kubeRef, error) {
	name, _ := raw["name"].(string)
	key, _ := raw["key"].(string)
	namespace, _ := raw["namespace"].(string)
	format, _ := raw["format"].(string)
	name = strings.TrimSpace(name)
	key = strings.TrimSpace(key)
	namespace = strings.TrimSpace(namespace)
	format = strings.TrimSpace(format)
	if name == "" || key == "" {
		return nil, fmt.Errorf("reference requires name and key")
	}
	if namespace == "" {
		namespace = defaultNamespace()
	}
	if namespace == "" {
		return nil, fmt.Errorf("namespace is required for reference %s:%s", name, key)
	}
	if format == "" {
		format = defaultRefFormat
	}
	return &kubeRef{
		Namespace: namespace,
		Name:      name,
		Key:       key,
		Format:    format,
	}, nil
}

func splitNamespaceName(input string) (string, string) {
	parts := strings.SplitN(input, "/", 2)
	if len(parts) == 2 {
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return "", strings.TrimSpace(input)
}

func defaultNamespace() string {
	for _, key := range []string{
		contracts.PodNamespaceEnv,
		contracts.StepRunNamespaceEnv,
		contracts.TargetStoryNamespaceEnv,
		contracts.ImpulseNamespaceEnv,
	} {
		if v := strings.TrimSpace(os.Getenv(key)); v != "" {
			return v
		}
	}
	return ""
}

func (sm *StorageManager) hydrateFromConfigMapRef(ctx context.Context, ref *kubeRef) (any, error) {
	client, err := kubeClient()
	if err != nil {
		return nil, err
	}
	cm, err := client.CoreV1().ConfigMaps(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to read ConfigMap %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	val, ok := cm.Data[ref.Key]
	if !ok {
		return nil, fmt.Errorf("ConfigMap %s/%s missing key %q", ref.Namespace, ref.Name, ref.Key)
	}
	return decodeRefValue([]byte(val), ref.Format, fmt.Sprintf("configmap %s/%s:%s", ref.Namespace, ref.Name, ref.Key))
}

func (sm *StorageManager) hydrateFromSecretRef(ctx context.Context, ref *kubeRef) (any, error) {
	client, err := kubeClient()
	if err != nil {
		return nil, err
	}
	secret, err := client.CoreV1().Secrets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to read Secret %s/%s: %w", ref.Namespace, ref.Name, err)
	}
	val, ok := secret.Data[ref.Key]
	if !ok {
		return nil, fmt.Errorf("Secret %s/%s missing key %q", ref.Namespace, ref.Name, ref.Key)
	}
	return decodeRefValue(val, ref.Format, fmt.Sprintf("secret %s/%s:%s", ref.Namespace, ref.Name, ref.Key))
}

func decodeRefValue(raw []byte, format, ref string) (any, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "raw":
		return string(raw), nil
	case "json":
		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("failed to decode JSON from %s: %w", ref, err)
		}
		return decoded, nil
	case "", defaultRefFormat:
		trimmed := strings.TrimSpace(string(raw))
		if trimmed == "" {
			return "", nil
		}
		var decoded any
		if json.Unmarshal([]byte(trimmed), &decoded) == nil {
			return decoded, nil
		}
		return string(raw), nil
	default:
		return nil, fmt.Errorf("unsupported format %q for %s", format, ref)
	}
}

var (
	kubeClientOnce sync.Once
	kubeClientInst *kubernetes.Clientset
	kubeClientErr  error
)

func kubeClient() (*kubernetes.Clientset, error) {
	kubeClientOnce.Do(func() {
		var cfg *rest.Config
		cfg, kubeClientErr = rest.InClusterConfig()
		if kubeClientErr != nil {
			cfg, kubeClientErr = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		}
		if kubeClientErr != nil {
			return
		}
		kubeClientInst, kubeClientErr = kubernetes.NewForConfig(cfg)
	})
	if kubeClientErr != nil {
		return nil, fmt.Errorf("failed to initialize Kubernetes client: %w", kubeClientErr)
	}
	return kubeClientInst, nil
}
