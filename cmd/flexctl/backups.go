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
	RunE: func(cmd *cobra.Command, args []string) error {
		if !backupsEnabled() {
			return fmt.Errorf("backups are not enabled")
		}

		return listBackups(cmd)
	},
	Args: cobra.NoArgs,
}

var backupCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates a new backup",
	Long:  `Creates a new backup.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !backupsEnabled() {
			return fmt.Errorf("backups are not enabled")
		}

		if err := createBackup(cmd); err != nil {
			return fmt.Errorf("failed to create backup: %v", err)
		}

		fmt.Println("Backup completed successfully!")

		return nil
	},
	Args: cobra.NoArgs,
}

var backupShowCmd = &cobra.Command{
	Use:   "show <backup-id>",
	Short: "Shows details about a specific backup",
	Long:  `Shows details about a specific backup.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if !backupsEnabled() {
			return fmt.Errorf("backups are not enabled")
		}
		return showBackup(cmd, args)
	},
	Args: cobra.ExactArgs(1),
}

func showBackup(cmd *cobra.Command, args []string) error {
	id := args[0]

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

	backupDetails, err := barman.ShowBackup(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to get backup details: %v", err)
	}

	fmt.Println(string(backupDetails))

	return nil
}

func createBackup(cmd *cobra.Command) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
	defer cancel()

	n, err := flypg.NewNode()
	if err != nil {
		return fmt.Errorf("failed to initialize node: %v", err)
	}

	conn, err := n.RepMgr.NewLocalConnection(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to local db: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()

	isPrimary, err := n.RepMgr.IsPrimary(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to determine if node is primary: %v", err)
	}

	if !isPrimary {
		return fmt.Errorf("backups can only be performed against the primary node")
	}

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
		return err
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
	table.SetHeader([]string{"ID/Name", "Alias", "Status", "End time", "Begin WAL"})

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
			b.ID,
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
