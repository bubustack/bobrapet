package templatesafety

import "testing"

func TestValidateTemplateStringRejectsDeniedFunctions(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "env", input: "{{ env \"HOME\" }}", want: true},
		{name: "expandenv", input: "{{ expandenv \"${HOME}\" }}", want: true},
		{name: "getHostByName", input: "{{ getHostByName \"example.com\" }}", want: true},
		{name: "allowed", input: "{{ printf \"ok\" }}", want: false},
		{name: "plain", input: "no templates here", want: false},
	}
	for _, tc := range cases {
		if err := ValidateTemplateString(tc.input); (err != nil) != tc.want {
			t.Fatalf("%s: error=%v wantErr=%v", tc.name, err, tc.want)
		}
	}
}

func TestValidateTemplateJSONRejectsDeniedFunctions(t *testing.T) {
	raw := []byte(`{"value":"{{ env \"HOME\" }}"}`)
	if err := ValidateTemplateJSON(raw); err == nil {
		t.Fatal("expected error for denied function in JSON template")
	}
}
