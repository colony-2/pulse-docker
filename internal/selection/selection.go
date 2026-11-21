package selection

import (
	"fmt"
	"regexp"
	"strings"
)

// MatchBase matches an entire concrete name, case-sensitively. Only '*' is
// special; it matches zero or more characters, including '/'. A selector of
// '*' is unconstrained, including when the allocation reports no base.
// Callers accepting external values must validate them before matching.
func MatchBase(selector, actual string) bool {
	if selector == "*" {
		return true
	}
	if actual == "" {
		return false
	}
	parts := strings.Split(selector, "*")
	if len(parts) == 1 {
		return selector == actual
	}
	if !strings.HasPrefix(actual, parts[0]) {
		return false
	}
	actual = actual[len(parts[0]):]
	for _, part := range parts[1 : len(parts)-1] {
		i := strings.Index(actual, part)
		if i < 0 {
			return false
		}
		actual = actual[i+len(part):]
	}
	return strings.HasSuffix(actual, parts[len(parts)-1])
}

var capabilityName = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[._/-][a-z0-9]+)*$`)

func ValidateCapabilities(names []string) error {
	for _, name := range names {
		if !capabilityName.MatchString(name) {
			return fmt.Errorf("execution.capabilities: invalid capability name %q", name)
		}
	}
	return nil
}
