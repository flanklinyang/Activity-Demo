package service

// BizError 业务错误：信息可直接展示给用户
type BizError struct{ Msg string }

func (e *BizError) Error() string { return e.Msg }

func newBizError(msg string) *BizError { return &BizError{Msg: msg} }

var (
	ErrUserExists      = newBizError("用户名已存在")
	ErrBadCredential   = newBizError("用户名或密码错误") // 不区分用户不存在/密码错误，防账号枚举
	ErrAlreadySignedUp = newBizError("你已经报名该活动")
	ErrQuotaExhausted  = newBizError("名额不足")
	ErrActivityMissing = newBizError("活动不存在或未开放报名")
)
