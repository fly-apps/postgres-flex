package api

type createUserRequest struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Superuser bool   `json:"superuser"`
	Database  string `json:"databases"`
}

type createDatabaseRequest struct {
	Name string `json:"name"`
}

type errRes struct {
	Error string `json:"error"`
}

type Response struct {
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}
