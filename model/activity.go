package model

import "gorm.io/gorm"

type Activity struct {
	gorm.Model
	Title      string `gorm:"size:128;not null"`
	Content    string `gorm:"type:text"`
	TotalQuota int64  `gorm:"not null"` // 总名额
	StartTime  int64
	EndTime    int64
	// 已报名人数不单独维护，读 Redis 报名集合 SCARD 获取
}

type ActivitySignUp struct {
	gorm.Model
	ActivityId int64 `gorm:"uniqueIndex:idx_activity_user"`
	UserId     int64 `gorm:"uniqueIndex:idx_activity_user"`
	// 联合唯一索引 (activity_id, user_id)，防止重复报名
}
