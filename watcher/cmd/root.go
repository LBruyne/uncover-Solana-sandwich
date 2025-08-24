package cmd

import (
	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "solana-mev-watcher",
	Short: "A tool for monitoring MEV on solana",
}
