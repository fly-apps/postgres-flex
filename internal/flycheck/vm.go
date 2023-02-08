package flycheck

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/superfly/fly-checks/check"
)

// CheckVM for system / disk checks
func CheckVM(checks *check.CheckSuite) *check.CheckSuite {
	ctx := context.Background()

	node, err := flypg.NewNode()
	if err != nil {
		return checks
	}

	// Check that provides additional insight into disk capacity and
	// how close we are to hitting the read-only threshold.
	checks.AddCheck("checkDisk", func() (string, error) {
		return checkDisk(ctx, node)
	})

	checks.AddCheck("checkLoad", func() (string, error) {
		return checkLoad()
	})

	pressureNames := []string{"memory", "cpu", "io"}
	for _, n := range pressureNames {
		name := n
		checks.AddCheck(name, func() (string, error) {
			return checkPressure(name)
		})
	}

	return checks
}

func checkPressure(name string) (string, error) {
	var avg10, avg60, avg300, counter float64
	//var rest string
	raw, err := ioutil.ReadFile("/proc/pressure/" + name)
	if err != nil {
		return "", err
	}

	fmt.Sscanf(
		string(raw),
		"some avg10=%f avg60=%f avg300=%f total=%f",
		&avg10, &avg60, &avg300, &counter,
	)

	avg10Dur, err := pressureToDuration(avg10, 10.0)
	if err != nil {
		return "", err
	}
	avg60Dur, err := pressureToDuration(avg60, 60.0)
	if err != nil {
		return "", err
	}

	avg300Dur, err := pressureToDuration(avg300, 300.0)
	if err != nil {
		return "", err
	}

	// Trigger failure if pressure exceeds 10 percent.
	if avg10 > 10 {
		return "", fmt.Errorf("system spent %s of the last 10 seconds waiting on %s", check.RoundDuration(avg10Dur, 2), name)
	}

	if avg60 > 10 {
		return "", fmt.Errorf("system spent %s of the last 60 seconds waiting on %s", check.RoundDuration(avg60Dur, 2), name)
	}

	if avg300 > 10 {
		return "", fmt.Errorf("system spent %s of the last 300 seconds waiting on %s", check.RoundDuration(avg300Dur, 2), name)
	}

	return fmt.Sprintf("system spent %s of the last 60s waiting on %s", check.RoundDuration(avg60Dur, 2), name), nil
}

func checkLoad() (string, error) {
	var loadAverage1, loadAverage5, loadAverage10 float64
	var runningProcesses, totalProcesses, lastProcessID int
	raw, err := ioutil.ReadFile("/proc/loadavg")

	if err != nil {
		return "", err
	}

	cpus := float64(runtime.NumCPU())
	_, err = fmt.Sscanf(string(raw), "%f %f %f %d/%d %d",
		&loadAverage1, &loadAverage5, &loadAverage10,
		&runningProcesses, &totalProcesses,
		&lastProcessID)
	if err != nil {
		return "", err
	}

	if loadAverage1/cpus > 10 {
		return "", fmt.Errorf("1 minute load average is very high: %.2f", loadAverage1)
	}
	if loadAverage5/cpus > 4 {
		return "", fmt.Errorf("5 minute load average is high: %.2f", loadAverage5)
	}
	if loadAverage10/cpus > 2 {
		return "", fmt.Errorf("10 minute load average is high: %.2f", loadAverage10)
	}

	return fmt.Sprintf("load averages: %.2f %.2f %.2f", loadAverage10, loadAverage5, loadAverage1), nil
}

func checkDisk(ctx context.Context, node *flypg.Node) (string, error) {
	// Calculate current disk usage
	size, available, err := diskUsage("/data/")
	if err != nil {
		return "", fmt.Errorf("failed to calculate disk usage: %s", err)
	}

	usedPercentage := float64(size-available) / float64(size) * 100

	// Turn primary read-only
	if usedPercentage > diskCapacityPercentageThreshold {
		// If the read-only lock has already been set, we can assume that we've already broadcasted.
		// TODO - This should be handled by the monitor service.
		if !flypg.ReadOnlyLockExists() {
			fmt.Println("Broadcasting read-only change to registered standbys")
			if err := flypg.BroadcastReadonlyChange(ctx, node, true); err != nil {
				fmt.Printf("errors with enable read-only broadcast: %s\n", err)
			}
		}

		return "", fmt.Errorf("%0.1f%% capacity - extend your volume to re-enable writes", usedPercentage)
	}

	// Don't attempt to disable read-only if there's a zombie.lock
	if !flypg.ZombieLockExists() && flypg.ReadOnlyLockExists() {
		if err := flypg.BroadcastReadonlyChange(ctx, node, false); err != nil {
			fmt.Printf("errors with disable read-only broadcast: %s\n", err)
		}
	}

	return fmt.Sprintf("%0.1f%% capacity", usedPercentage), nil
}

func diskUsage(dir string) (size uint64, available uint64, err error) {
	var stat syscall.Statfs_t

	if err = syscall.Statfs(dir, &stat); err != nil {
		return 0, 0, fmt.Errorf("%s: %s", dir, err)
	}

	size = stat.Blocks * uint64(stat.Bsize)
	available = stat.Bavail * uint64(stat.Bsize)

	return size, available, nil
}

func round(val float64, roundOn float64, places int) (newVal float64) {
	var round float64
	pow := math.Pow(10, float64(places))
	digit := pow * val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}
	newVal = round / pow
	return
}
func dataSize(size uint64) string {
	var suffixes [5]string
	suffixes[0] = "B"
	suffixes[1] = "KB"
	suffixes[2] = "MB"
	suffixes[3] = "GB"
	suffixes[4] = "TB"

	base := math.Log(float64(size)) / math.Log(1024)
	getSize := round(math.Pow(1024, base-math.Floor(base)), .5, 2)
	getSuffix := suffixes[int(math.Floor(base))]
	return fmt.Sprint(strconv.FormatFloat(getSize, 'f', -1, 64) + " " + getSuffix)
}

func pressureToDuration(pressure float64, base float64) (time.Duration, error) {
	seconds := base * (pressure / 100)
	secondsStr := fmt.Sprintf("%fs", seconds)
	return time.ParseDuration(secondsStr)
}
