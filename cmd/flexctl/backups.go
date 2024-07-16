package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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

func newBackupConfigCmd() *cobra.Command {
	var configCmd = &cobra.Command{
		Use:   "config",
		Short: "Manage backup configuration",
	}

	configCmd.AddCommand(newConfigShowCmd())
	configCmd.AddCommand(newConfigUpdateCmd())

	return configCmd
}

type configShowResult struct {
	Result flypg.BarmanSettings `json:"result"`
}

func newConfigShowCmd() *cobra.Command {
	var configShowCmd = &cobra.Command{
		Use:   "show",
		Short: "Show current configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := http.Get("http://localhost:5500/commands/admin/settings/view/barman")
			if err != nil {
				return err
			}

			var rv configShowResult
			err = json.NewDecoder(resp.Body).Decode(&rv)
			if err != nil {
				return err
			}

			fmt.Printf("  ArchiveTimeout = %s\n", rv.Result.ArchiveTimeout)
			fmt.Printf("  RecoveryWindow = %s\n", rv.Result.RecoveryWindow)
			fmt.Printf("  FullBackupFrequency = %s\n", rv.Result.FullBackupFrequency)
			fmt.Printf("  MinimumRedundancy = %s\n", rv.Result.MinimumRedundancy)

			return nil
		},
	}

	return configShowCmd
}

type successfulUpdateResult struct {
	Message         string `json:"message,omitempty"`
	RestartRequired bool   `json:"restart_required"`
}

type configUpdateResult struct {
	Result successfulUpdateResult `json:"result,omitempty"`
	Error  string                 `json:"error,omitempty"`
}

func newConfigUpdateCmd() *cobra.Command {
	var configUpdateCmd = &cobra.Command{
		Use:   "update",
		Short: "Update configuration",
		RunE: func(cmd *cobra.Command, args []string) error {
			update := flypg.BarmanSettings{
				ArchiveTimeout:      cmd.Flag("archive-timeout").Value.String(),
				RecoveryWindow:      cmd.Flag("recovery-window").Value.String(),
				FullBackupFrequency: cmd.Flag("full-backup-frequency").Value.String(),
				MinimumRedundancy:   cmd.Flag("minimum-redundancy").Value.String(),
			}

			jsonBody, err := json.Marshal(update)
			if err != nil {
				return err
			}

			resp, err := http.Post("http://localhost:5500/commands/admin/settings/update/barman", "application/json", bytes.NewBuffer(jsonBody))
			if err != nil {
				return err
			}

			var rv configUpdateResult
			err = json.NewDecoder(resp.Body).Decode(&rv)
			if err != nil {
				return err
			}

			if rv.Error != "" {
				return fmt.Errorf("error updating configuration: %s", rv.Error)
			}

			if rv.Result.Message != "" {
				fmt.Println(rv.Result.Message)
			}

			if rv.Result.RestartRequired {
				fmt.Println("A restart is required for these changes to take effect.")
			}

			return nil
		},
	}

	configUpdateCmd.Flags().StringP("archive-timeout", "", "", "Archive timeout")
	configUpdateCmd.Flags().StringP("recovery-window", "", "", "Recovery window")
	configUpdateCmd.Flags().StringP("full-backup-frequency", "", "", "Full backup frequency")
	configUpdateCmd.Flags().StringP("minimum-redundancy", "", "", "Minimum redundancy")

	configUpdateCmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		requiredFlags := []string{"archive-timeout", "recovery-window", "full-backup-frequency", "minimum-redundancy"}
		providedFlags := 0

		for _, flag := range requiredFlags {
			if cmd.Flag(flag).Changed {
				providedFlags++
			}
		}

		if providedFlags < 1 {
			return fmt.Errorf("at least one flag must be specified")
		}

		return nil
	}

	return configUpdateCmd
}
