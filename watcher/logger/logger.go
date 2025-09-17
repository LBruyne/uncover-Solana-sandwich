package logger

import (
	"bufio"
	"context"
	"log"
	"log/slog"
	"os"
	"time"
	"watcher/config"
)

var consoleEnabled = true

const MaxLogSize = 100 * 1024 * 1024 // 100 MB

var (
	SolLogger    *slog.Logger
	JitoLogger   *slog.Logger
	GlobalLogger *slog.Logger

	solBuf, jitoBuf, globalBuf    *bufio.Writer
	solFile, jitoFile, globalFile *os.File
)

type bufferedHandler struct {
	handler slog.Handler
	writer  *bufio.Writer
}

func (b *bufferedHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return b.handler.Enabled(ctx, lvl)
}

func (b *bufferedHandler) Handle(ctx context.Context, r slog.Record) error {
	if err := b.handler.Handle(ctx, r); err != nil {
		return err
	}
	return b.writer.Flush()
}

func (b *bufferedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &bufferedHandler{
		handler: b.handler.WithAttrs(attrs),
		writer:  b.writer,
	}
}

func (b *bufferedHandler) WithGroup(name string) slog.Handler {
	return &bufferedHandler{
		handler: b.handler.WithGroup(name),
		writer:  b.writer,
	}
}

type multiHandler struct {
	handlers []slog.Handler
}

func (m *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range m.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (m *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var lastErr error
	for _, h := range m.handlers {
		if err := h.Handle(ctx, r); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

func (m *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newHandlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		newHandlers[i] = h.WithAttrs(attrs)
	}
	return &multiHandler{handlers: newHandlers}
}

func (m *multiHandler) WithGroup(name string) slog.Handler {
	newHandlers := make([]slog.Handler, len(m.handlers))
	for i, h := range m.handlers {
		newHandlers[i] = h.WithGroup(name)
	}
	return &multiHandler{handlers: newHandlers}
}

func FlushAll() {
	if solBuf != nil {
		solBuf.Flush()
	}
	if jitoBuf != nil {
		jitoBuf.Flush()
	}
	if globalBuf != nil {
		globalBuf.Flush()
	}
}

func CloseAll() {
	FlushAll()
	if solFile != nil {
		solFile.Close()
	}
	if jitoFile != nil {
		jitoFile.Close()
	}
	if globalFile != nil {
		globalFile.Close()
	}
}

func SetConsoleEnabled(enabled bool) {
	consoleEnabled = enabled

	if GlobalLogger != nil && globalBuf != nil {
		GlobalLogger = slog.New(&multiHandler{handlers: makeHandlers(globalBuf, consoleEnabled)})
	}
	if SolLogger != nil && solBuf != nil {
		SolLogger = slog.New(&multiHandler{handlers: makeHandlers(solBuf, consoleEnabled)})
	}
	if JitoLogger != nil && jitoBuf != nil {
		JitoLogger = slog.New(&multiHandler{handlers: makeHandlers(jitoBuf, consoleEnabled)})
	}
}

func makeHandlers(fileBuf *bufio.Writer, toConsole bool) []slog.Handler {
	hs := make([]slog.Handler, 0, 2)
	if toConsole {
		hs = append(hs, &bufferedHandler{
			handler: slog.NewTextHandler(os.Stdout, nil),
			writer:  bufio.NewWriter(os.Stdout),
		})
	}
	hs = append(hs, &bufferedHandler{
		handler: slog.NewTextHandler(fileBuf, nil),
		writer:  fileBuf,
	})
	return hs
}

func openLogFile(path string) (*os.File, *bufio.Writer, error) {
	if info, err := os.Stat(path); err == nil && info.Size() > MaxLogSize {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
		if err != nil {
			return nil, nil, err
		}
		return file, bufio.NewWriter(file), nil
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, nil, err
	}
	return file, bufio.NewWriter(file), nil
}

func init() {
	var err error

	// Create ./logs directory if not exists
	if _, err := os.Stat(config.LogPath); os.IsNotExist(err) {
		err = os.Mkdir(config.LogPath, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}

	timestamp := time.Now().Format("20060102_150405")
	globalFile, globalBuf, err = openLogFile(config.LogPath + "watcher_" + timestamp + "_global.log")
	if err != nil {
		log.Fatal(err)
	}

	// GlobalLogger
	GlobalLogger = slog.New(&multiHandler{
		handlers: makeHandlers(globalBuf, consoleEnabled),
	})
}

func InitLogs(cmdName string) {
	var err error

	// Create ./logs directory if not exists
	if _, err := os.Stat(config.LogPath); os.IsNotExist(err) {
		err = os.Mkdir(config.LogPath, 0755)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Filename is like: "watcher_20231010_150405_tool_sol.log"
	timestamp := time.Now().Format("20060102_150405")

	solFile, solBuf, err = openLogFile(config.LogPath + "watcher_" + timestamp + "_" + cmdName + "_sol.log")
	if err != nil {
		log.Fatal(err)
	}
	jitoFile, jitoBuf, err = openLogFile(config.LogPath + "watcher_" + timestamp + "_" + cmdName + "_jito.log")
	if err != nil {
		log.Fatal(err)
	}

	// SolLogger
	SolLogger = slog.New(&multiHandler{
		handlers: makeHandlers(solBuf, consoleEnabled),
	})
	// JitoLogger
	JitoLogger = slog.New(&multiHandler{
		handlers: makeHandlers(jitoBuf, consoleEnabled),
	})
}
