package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:           "flexctl",
		SilenceErrors: true,
		SilenceUsage:  true,
	}

	// Backup commands
	var backupCmd = &cobra.Command{Use: "backup"}
	backupCmd.Aliases = []string{"backups"}

	rootCmd.AddCommand(backupCmd)
	backupCmd.AddCommand(backupListCmd)
	backupCmd.AddCommand(backupShowCmd)
	backupCmd.AddCommand(backupCreateCmd)
	backupCmd.AddCommand(newBackupConfig())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	// Backup list
	backupListCmd.Flags().StringP("status", "s", "", "Filter backups by status (Not applicable for JSON output)")
	backupListCmd.Flags().BoolP("json", "", false, "Output in JSON format")
	// Backup show
	backupShowCmd.Flags().BoolP("json", "", false, "Output in JSON format")
	// Backup create
	backupCreateCmd.Flags().StringP("name", "n", "", "Name of the backup")
	backupCreateCmd.Flags().BoolP("immediate-checkpoint", "", false, "Forces Postgres to perform an immediate checkpoint")
}
