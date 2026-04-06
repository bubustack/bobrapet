package identity

import "fmt"

// DeriveEffectClaimName returns the deterministic EffectClaim name for the
// provided StepRun namespace/name and effect key.
func DeriveEffectClaimName(stepRunNamespace, stepRunName, effectKey string) string {
	if effectKey == "" {
		return ""
	}

	hashSeed := stepRunNamespace + "/" + stepRunName + "/" + effectKey
	hashed := shortTokenHash(hashSeed)
	base := fmt.Sprintf("%s-effect-%s", stepRunName, hashed)
	if len(base) <= storyRunNameMaxLength {
		return base
	}

	maxPrefix := max(storyRunNameMaxLength-len("-effect-")-len(hashed), 1)
	prefix := stepRunName
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
		prefix = trimTrailingHyphen(prefix)
		if prefix == "" {
			prefix = "s"
		}
	}
	return fmt.Sprintf("%s-effect-%s", prefix, hashed)
}
