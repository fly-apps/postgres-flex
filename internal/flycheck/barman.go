package flycheck

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/superfly/fly-checks/check"
)

func CheckBarmanConnection(checks *check.CheckSuite) *check.CheckSuite {
	cmd := exec.Command("barman", "check", "pg")

	output, err := cmd.CombinedOutput()
	if err != nil {
		checks.AddCheck("connection", func() (string, error) {
			msg := fmt.Sprintf("Failed running `barman check pg`")
			return "", errors.New(msg)
		})

		return checks
	}

	// Each line besides the first represents a check and will include FAILED or OK
	// We just separate those lines and create a health check entry of our own
	// so it's uniform how we handle it
	lines := strings.Split(string(output), "\n")

	for _, line := range lines {
		pattern := `\s*(.*?):(.*)$`
		regex := regexp.MustCompile(pattern)
		matches := regex.FindStringSubmatch(line)

		if len(matches) == 3 {
			left := matches[1]
			right := strings.Trim(matches[2], "")

			if right == "" {
				continue
			}

			checks.AddCheck(left, func() (string, error) {
				if strings.Contains(right, "FAILED") {
					return "", errors.New(right)
				}

				return right, nil
			})
		}
	}

	return checks
}
