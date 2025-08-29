package cmd

import (
	"watcher/jito"
	"watcher/logger"

	"github.com/spf13/cobra"
)

// var startSlot uint64

var jitoCmd = cobra.Command{
	Use:   "jito",
	Short: "Start monitoring, sycning and storing Jito bundles",
	Run: func(cmd *cobra.Command, args []string) {
		logger.JitoLogger.Info("Running cmd jito, starting Jito bundle monitoring...")

		// if startSlot > 0 {
		// 	if startSlot < config.MIN_START_SLOT {
		// 		err = fmt.Errorf("starting slot must be >= %d", config.MIN_START_SLOT)
		// 	}
		// 	err = jito.RunJitoCmd(startSlot)
		// } else {
		// }
		if err := jito.RunJitoCmd(); err != nil {
			logger.JitoLogger.Error("Error running Jito command: %v", err)
		}
	},
}

func init() {
	// jitoCmd.Flags().Uint64Var(&startSlot, "slot", 0, "starting slot number (>=330000000), optional")
	RootCmd.AddCommand(&jitoCmd)
}
