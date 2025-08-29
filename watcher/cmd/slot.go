package cmd

import (
	"fmt"
	"watcher/config"
	"watcher/logger"
	"watcher/sol"

	"github.com/spf13/cobra"
)

var startSlot uint64

var slotCmd = cobra.Command{
	Use:   "slot-info",
	Short: "Start monitoring, syncing and storing Slot information",
	Run: func(cmd *cobra.Command, args []string) {
		if startSlot < config.MIN_START_SLOT {
			logger.SolLogger.Error(fmt.Sprintf("start slot (%d) is below minimum allowed slot (%d)", startSlot, config.MIN_START_SLOT))
			return
		}
		// if endSlot > 0 && endSlot < config.MIN_START_SLOT {
		// 	logger.SolLogger.Error(fmt.Sprintf("end slot (%d) is below minimum allowed slot (%d)", endSlot, config.MIN_START_SLOT))
		// 	return
		// }
		// if endSlot > 0 && startSlot > endSlot {
		// 	logger.SolLogger.Error(fmt.Sprintf("start slot (%d) cannot be greater than end slot (%d)", startSlot, endSlot))
		// 	return
		// }

		logger.SolLogger.Info("Running cmd slot-info, starting slot information monitoring...", "start", startSlot)

		if err := sol.RunSlotCmd(startSlot); err != nil {
			logger.SolLogger.Error("Error running Slot command: %v", err)
		}
	},
}

func init() {
	slotCmd.Flags().Uint64VarP(
		&startSlot,
		"slot",
		"s",
		0,
		fmt.Sprintf("(Optional) starting slot number (>=%d)", config.MIN_START_SLOT),
	)
	// slotCmd.Flags().Uint64P("end", "e", 0, "end slot")
	RootCmd.AddCommand(&slotCmd)
}
