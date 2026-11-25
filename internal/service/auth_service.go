package service

import (
	"activity/middleware"
	"activity/model"
	"activity/repository"
	"errors"

	"golang.org/x/crypto/bcrypt"
)

func Register(username, password string) error {
	var u model.User
	res := repository.DB.Where("username = ?", username).First(&u)
	if res.Error == nil {
		return errors.New("用户名已存在")
	}
	hashPwd, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	user := model.User{
		Username: username,
		Password: string(hashPwd),
		Role:     "student",
	}
	return repository.DB.Create(&user).Error
}

func Login(username, password string) (string, error) {
	var u model.User
	res := repository.DB.Where("username = ?", username).First(&u)
	if res.Error != nil {
		return "", errors.New("用户不存在")
	}
	err := bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(password))
	if err != nil {
		return "", errors.New("密码错误")
	}
	token, err := middleware.GenerateToken(int64(u.ID), u.Username, u.Role)
	return token, err
}
