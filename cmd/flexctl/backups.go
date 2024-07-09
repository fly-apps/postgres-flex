package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/fly-apps/postgres-flex/internal/flypg"
	"github.com/fly-apps/postgres-flex/internal/flypg/state"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
)

var backupListCmd = &cobra.Command{
	Use:   "list",
	Short: "Lists all backups",
	Long:  `Lists all available backups created.`,
	Run: func(cmd *cobra.Command, args []string) {
		if !backupsEnabled() {
			fmt.Fprintln(os.Stderr, "Backups are not enabled.")
			os.Exit(1)
		}

		if err := listBackups(cmd); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}

		os.Exit(0)
	},
	Args: cobra.NoArgs,
}

var backupCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates a new backup",
	Long:  `Creates a new backup.`,
	Run: func(cmd *cobra.Command, args []string) {
		if !backupsEnabled() {
			fmt.Fprintln(os.Stderr, "Backups are not enabled.")
			os.Exit(1)
		}

		if err := createBackup(cmd); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		fmt.Println("Backup completed successfully!")

		os.Exit(0)
	},
	Args: cobra.NoArgs,
}

var backupShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Shows details about a specific backup",
	Long:  `Shows details about a specific backup.`,
	Run: func(cmd *cobra.Command, args []string) {
		if !backupsEnabled() {
			fmt.Fprintln(os.Stderr, "Backups are not enabled.")
			os.Exit(1)
		}

		if err := showBackup(cmd, args); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		os.Exit(0)
	},
	Args: cobra.ExactArgs(1),
}

func showBackup(cmd *cobra.Command, args []string) error {
	id := args[0]

	if id == "" {
		return fmt.Errorf("backup ID is required")
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()

	store, err := state.NewStore()
	if err != nil {
		return fmt.Errorf("failed to initialize store: %v", err)
	}

	barman, err := flypg.NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), flypg.DefaultAuthProfile)
	if err != nil {
		return fmt.Errorf("failed to initialize barman: %v", err)
	}

	backupDetails, err := barman.BackupDetails(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get backup details: %v", err)
	}

	fmt.Println(string(backupDetails))

	return nil
}

func createBackup(cmd *cobra.Command) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
	defer cancel()

	store, err := state.NewStore()
	if err != nil {
		return fmt.Errorf("failed to initialize store: %v", err)
	}

	barman, err := flypg.NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), flypg.DefaultAuthProfile)
	if err != nil {
		return fmt.Errorf("failed to initialize barman: %v", err)
	}

	name, err := cmd.Flags().GetString("name")
	if err != nil {
		return fmt.Errorf("failed to get name flag: %v", err)
	}

	immediateCheckpoint, err := cmd.Flags().GetBool("immediate-checkpoint")
	if err != nil {
		return fmt.Errorf("failed to get immediate-checkpoint flag: %v", err)
	}

	cfg := flypg.BackupConfig{
		ImmediateCheckpoint: immediateCheckpoint,
		Name:                name,
	}

	fmt.Println("Performing backup...")

	if _, err := barman.Backup(ctx, cfg); err != nil {
		return fmt.Errorf("failed to create backup: %v", err)
	}

	return nil
}

func listBackups(cmd *cobra.Command) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
	defer cancel()

	store, err := state.NewStore()
	if err != nil {
		return fmt.Errorf("failed to initialize store: %v", err)
	}

	barman, err := flypg.NewBarman(store, os.Getenv("S3_ARCHIVE_CONFIG"), flypg.DefaultAuthProfile)
	if err != nil {
		return fmt.Errorf("failed to initialize barman: %v", err)
	}

	isJSON, err := cmd.Flags().GetBool("json")
	if err != nil {
		return fmt.Errorf("failed to get json flag: %v", err)
	}

	if isJSON {
		jsonBytes, err := barman.ListRawBackups(cmd.Context())
		if err != nil {
			return fmt.Errorf("failed to list backups: %v", err)
		}

		fmt.Println(string(jsonBytes))
		return nil
	}

	backupList, err := barman.ListBackups(ctx)
	if err != nil {
		return fmt.Errorf("failed to list backups: %v", err)
	}

	if len(backupList.Backups) == 0 {
		fmt.Println("No backups found")
		return nil
	}

	var filterStatus string

	filterStatus, err = cmd.Flags().GetString("status")
	if err != nil {
		return fmt.Errorf("failed to get status flag: %v", err)
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Name", "Status", "End time", "Begin WAL"})

	// Set table alignment, borders, padding, etc. as needed
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(true) // Set to false to hide borders
	table.SetCenterSeparator("|")
	table.SetColumnSeparator("|")
	table.SetRowSeparator("-")
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetHeaderLine(true) // Enable header line
	table.SetAutoWrapText(false)

	for _, b := range backupList.Backups {
		if filterStatus != "" && b.Status != filterStatus {
			continue
		}

		table.Append([]string{
			b.BackupID,
			b.Name,
			b.Status,
			b.EndTime,
			b.BeginWal,
		})
	}

	table.Render()

	return nil
}

func backupsEnabled() bool {
	return os.Getenv("S3_ARCHIVE_CONFIG") != ""
}
