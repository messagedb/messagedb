package bindings

type RegisterNewUser struct {
	Username     string `json:"username" binding:"required"`
	EmailAddress string `json:"email" binding:"required,email"`
	Password     string `json:"password" binding:"required"`
}

type AuthorizeUser struct {
	Login    string `json:"login" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type RefreshToken struct {
	Token string `json:"refresh_token" binding:"required"`
}

type UpdateUser struct {
	GivenName  string `json:"given_name,omitempty" binding:"required"`
	FamilyName string `json:"family_name,omitempty" binding:"required"`
}

type ChangeUsername struct {
	Username string `json:"username" binding:"required"`
}

type ChangePassword struct {
	OldPassword string `json:"old_password" binding:"required"`
	NewPassword string `json:"new_password" binding:"required"`
}

type UpdateEmail struct {
	Email string `json:"email" binding:"required"`
}
