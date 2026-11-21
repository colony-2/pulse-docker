package quantity

import (
	"fmt"
	"strings"
)

func CPU(n int64) string { return fmt.Sprintf("%dm", n) }
func Bytes(n int64) string {
	if n%1024 == 0 {
		return fmt.Sprintf("%dKi", n/1024)
	}
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%d.%03d", n/1000, n%1000), "0"), ".") + "k"
}
