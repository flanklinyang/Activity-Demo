package logger

import "go.uber.org/zap"

// L 全局结构化日志（JSON，生产默认）
var L *zap.SugaredLogger

func Init() error {
	z, err := zap.NewProduction()
	if err != nil {
		return err
	}
	L = z.Sugar()
	return nil
}

// Sync 刷新日志缓冲，退出前调用
func Sync() {
	if L != nil {
		_ = L.Sync()
	}
}
