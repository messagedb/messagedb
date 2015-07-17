package services

import (
	"errors"
	"fmt"
	"time"

	"github.com/messagedb/messagedb/meta/bindings"
	"github.com/messagedb/messagedb/meta/schema"

	jwt "github.com/dgrijalva/jwt-go"
)

// Secrets
var (
	signingKey = []byte("secret")
	refreshKey = []byte("refreshsecret")
)

// Errors
var (
	ErrInvalidAccessToken  = errors.New("Invalid Access Token")
	ErrInvalidRefreshToken = errors.New("Invalid Refresh Token")
)

// Auth is the singleton instance for the Auth service
var Auth = &authService{signingKey, refreshKey}

type authService struct {
	SigningKey []byte
	RefreshKey []byte
}

// TokenFields represents the security tokens that gets generated and sent as API response
type TokenFields struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	ExpiresAt    time.Time `json:"expires_at"`
}

func (a *authService) GenerateToken(user *schema.User) (*TokenFields, error) {

	expiresAt := time.Now().Add(time.Hour * 2)

	token := jwt.New(jwt.GetSigningMethod("HS256"))
	token.Claims["uid"] = user.ID
	token.Claims["uname"] = user.Username
	token.Claims["iat"] = expiresAt.Unix()

	accessToken, err := token.SignedString(a.SigningKey)
	if err != nil {
		return nil, err
	}

	// generate JWT access token
	token = jwt.New(jwt.GetSigningMethod("HS256"))
	token.Claims["uid"] = user.ID
	token.Claims["uname"] = user.Username
	token.Claims["iat"] = time.Now().Add(time.Hour * 24 * 14).Unix()

	refreshToken, err := token.SignedString(a.RefreshKey)
	if err != nil {
		return nil, err
	}

	return &TokenFields{AccessToken: accessToken, RefreshToken: refreshToken, ExpiresAt: expiresAt}, nil
}

func (a *authService) AuthorizeUser(credentials bindings.AuthorizeUser) (*schema.User, error) {
	// var user *schema.User
	// var err error
	//
	// // first check if the login credentials used is an email, otherwise use username to locate user
	// if valid.IsEmail(credentials.Login) {
	// 	user, err = models.User.FindByEmail(credentials.Login)
	// } else {
	// 	user, err = models.User.FindByUsername(credentials.Login)
	// }
	// if err != nil {
	// 	return nil, err
	// }
	//
	// // if we cannot find the user, return an error indicating that authentication has failed
	// if user == nil {
	// 	return nil, ErrAuthenticationFailedUserNotFound
	// }
	//
	// // Lets try to validate the credentials against the database
	// ok, err := user.ValidatePassword(credentials.Password)
	// if err != nil {
	// 	return user, ErrAuthenticationFailedValidationError
	// }
	//
	// // if not OK then the password did not match
	// if !ok {
	// 	return user, ErrAuthenticationFailedPasswordMismatch
	// }
	//
	// return user, nil
	return nil, nil
}

func (a *authService) ValidateAccessToken(accessToken string) (*schema.User, error) {

	// 	token, err := jwt.Parse(accessToken, a.validateAccessTokenFunc)
	// 	if err != nil || !token.Valid {
	// 		return nil, ErrInvalidRefreshToken
	// 	}
	//
	// 	userID := token.Claims["uid"]
	// 	user, err := models.User.FindById(userID.(string))
	// 	if err != nil {
	// 		return nil, fmt.Errorf("Unexpected loading user: %s", userID)
	// 	}
	//
	// 	if user == nil {
	// 		return nil, fmt.Errorf("Unable to find user: %s", userID)
	// 	}
	//
	// 	return user, nil
	// }
	//
	// func (a *authService) ValidateRefreshToken(refreshToken string) (*schema.User, error) {
	//
	// 	token, err := jwt.Parse(refreshToken, a.validateRefreshTokenFunc)
	// 	if err != nil || !token.Valid {
	// 		return nil, ErrInvalidRefreshToken
	// 	}
	//
	// 	userID := token.Claims["uid"]
	// 	user, err := models.User.FindById(userID.(string))
	// 	if err != nil {
	// 		return nil, fmt.Errorf("Unexpected loading user: %s", userID)
	// 	}
	//
	// 	if user == nil {
	// 		return nil, fmt.Errorf("Unable to find user: %s", userID)
	// 	}
	//
	// 	return user, nil
	return nil, nil
}

func (a *authService) validateAccessTokenFunc(token *jwt.Token) (interface{}, error) {
	if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
	}
	return a.SigningKey, nil
}

func (a *authService) validateRefreshTokenFunc(token *jwt.Token) (interface{}, error) {
	if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
	}
	return a.RefreshKey, nil
}
