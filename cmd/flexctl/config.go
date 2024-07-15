package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newConfigCmd() *cobra.Command {
	var configCmd = &cobra.Command{
		Use:   "config",
		Short: "Manage Postgres configuration",
	}

	configCmd.AddCommand(newConfigShowCmd())
	configCmd.AddCommand(newConfigUpdateCmd())

	return configCmd
}

func newConfigShowCmd() *cobra.Command {
	var configShowCmd = &cobra.Command{
		Use:   "show",
		Short: "Show current configuration",
		Run: func(cmd *cobra.Command, args []string) {
			// Add your logic here for showing the configuration
			fmt.Println("Showing configuration...")
		},
	}

	return configShowCmd
}

func newConfigUpdateCmd() *cobra.Command {
	var configUpdateCmd = &cobra.Command{
		Use:   "update",
		Short: "Update configuration",
		Run: func(cmd *cobra.Command, args []string) {
			// Add your logic here for updating the configuration
			fmt.Println("Updating configuration...")
		},
	}

	return configUpdateCmd
}
