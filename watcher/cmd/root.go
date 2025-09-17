package cmd

import (
	"fmt"
	"watcher/config"
	"watcher/logger"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "solana-mev-watcher",
	Short: "A tool for monitoring MEV on solana",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		logger.SetConsoleEnabled(!notToStdout)
	},
}

// Flags
var jitoStart uint64
var slotStart uint64
var sandwichStart uint64
var notToStdout bool

func init() {

	RootCmd.PersistentFlags().BoolVarP(
		&notToStdout,
		"no-stdout",
		"t",
		false,
		"Do not write logs to stdout (terminal) output (default false)",
	)

	jitoCmd.Flags().Uint64VarP(
		&jitoStart,
		"slot",
		"s",
		0,
		fmt.Sprintf("(Optional) starting slot number (>=%d)", config.MIN_START_SLOT),
	)

	slotCmd.Flags().Uint64VarP(
		&slotStart,
		"slot",
		"s",
		0,
		fmt.Sprintf("(Optional) starting slot number (>=%d)", config.MIN_START_SLOT),
	)

	sandwichCmd.Flags().Uint64VarP(
		&sandwichStart,
		"slot",
		"s",
		0,
		fmt.Sprintf("(Optional) starting slot number (>=%d)", config.MIN_START_SLOT),
	)

	RootCmd.AddCommand(&resetCmd, &jitoCmd, &slotCmd, &sandwichCmd)
}
