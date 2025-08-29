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
	globalFile, err = os.OpenFile(config.LogPath+"watcher_"+timestamp+"_global.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}

	globalBuf = bufio.NewWriter(globalFile)
	// GlobalLogger
	GlobalLogger = slog.New(&multiHandler{
		handlers: []slog.Handler{
			&bufferedHandler{handler: slog.NewTextHandler(os.Stdout, nil), writer: bufio.NewWriter(os.Stdout)},
			&bufferedHandler{handler: slog.NewTextHandler(globalBuf, nil), writer: globalBuf},
		},
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
	solFile, err = os.OpenFile(config.LogPath+"watcher_"+timestamp+"_"+cmdName+"_sol.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	jitoFile, err = os.OpenFile(config.LogPath+"watcher_"+timestamp+"_"+cmdName+"_sol.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}

	solBuf = bufio.NewWriter(solFile)
	jitoBuf = bufio.NewWriter(jitoFile)

	// SolLogger
	SolLogger = slog.New(&multiHandler{
		handlers: []slog.Handler{
			&bufferedHandler{handler: slog.NewTextHandler(os.Stdout, nil), writer: bufio.NewWriter(os.Stdout)},
			&bufferedHandler{handler: slog.NewTextHandler(solBuf, nil), writer: solBuf},
		},
	})

	// JitoLogger
	JitoLogger = slog.New(&multiHandler{
		handlers: []slog.Handler{
			&bufferedHandler{handler: slog.NewTextHandler(os.Stdout, nil), writer: bufio.NewWriter(os.Stdout)},
			&bufferedHandler{handler: slog.NewTextHandler(jitoBuf, nil), writer: jitoBuf},
		},
	})
}
