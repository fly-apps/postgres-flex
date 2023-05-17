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
	cmd := exec.Command("cat", "/Users/lubien/workspace/postgres-flex/test.txt")

	output, err := cmd.CombinedOutput()
	if err != nil {
		checks.AddCheck("connection", func() (string, error) {
			msg := fmt.Sprintf("Failed running `barman check pg`")
			return "", errors.New(msg)
		})

		return checks
	}

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
