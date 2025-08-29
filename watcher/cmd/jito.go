package cmd

import (
	"watcher/jito"
	"watcher/logger"

	"github.com/spf13/cobra"
)

var jitoCmd = cobra.Command{
	Use:   "jito",
	Short: "Start monitoring, sycning and storing Jito bundles",
	Run: func(cmd *cobra.Command, args []string) {
		logger.JitoLogger.Info("Running cmd jito, starting Jito bundle monitoring...")

		if err := jito.RunJitoCmd(); err != nil {
			logger.JitoLogger.Error("Error running Jito command", "error", err)
		}
	},
}
