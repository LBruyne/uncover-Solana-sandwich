package cmd

import (
	"fmt"
	"watcher/config"

	"github.com/spf13/cobra"
)

var RootCmd = &cobra.Command{
	Use:   "solana-mev-watcher",
	Short: "A tool for monitoring MEV on solana",
}

// Flags
var jitoStart uint64
var slotStart uint64
var sandwichStart uint64

func init() {

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
