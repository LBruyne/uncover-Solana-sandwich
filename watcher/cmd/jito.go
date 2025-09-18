package cmd

import (
	"fmt"
	"watcher/config"
	"watcher/jito"
	"watcher/logger"

	"github.com/spf13/cobra"
)

var jitoCmd = cobra.Command{
	Use:   "jito",
	Short: "Start monitoring, sycning and storing Jito bundles, including marking sandwiches in Jito bundles",
	Run: func(cmd *cobra.Command, args []string) {
		logger.InitLogs("jito")

		if jitoStart < config.MIN_START_SLOT {
			logger.JitoLogger.Error(fmt.Sprintf("start slot (%d) is below minimum allowed slot (%d)", jitoStart, config.MIN_START_SLOT))
			return
		}

		logger.JitoLogger.Info("Running cmd jito, starting Jito bundle monitoring and marking...")

		if err := jito.RunJitoCmd(jitoStart, !disableJitoTask1, !disableJitoTask2); err != nil {
			logger.JitoLogger.Error("Error running Jito command", "error", err)
		}
	},
}
