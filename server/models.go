package server

type ExistsResponse struct {
	Exists bool `json:"exists"`
}

type NewDB struct {
	Name string `json:"name" validate:"required,alphanum,min=1,max=100"`
}

type NewDBCreated struct {
	Name    string `json:"name" validate:"required,alphanum,min=1,max=100"`
	Created bool   `json:"created"`
	ApiKey  string `json:"api_key"`
	Exists  bool   `json:"exists"`
}

type NewLiFoFifo struct {
	Name  string `json:"name" validate:"required,alphanum,min=1,max=100"`
	Limit int    `json:"limit" validate:"required,min=1,max=2000000"`
}

type DeleteFiFoLiFo struct {
	ApiKey string `json:"api_key"`
	Name   string `json:"name" validate:"required,alphanum,min=1,max=100"`
}

type PushFiFoLiFo struct {
	ApiKey string `json:"api_key"`
	Name   string `json:"name" validate:"required,alphanum,min=1,max=100"`
	Value  string `json:"value" validate:"required,min=1,max=30000"`
}

type PopFiFoLiFo struct {
	ApiKey string `json:"api_key"`
	Name   string `json:"name" validate:"required,alphanum,min=1,max=100"`
}

type Set struct {
	ApiKey string `json:"api_key"`
	Ttl    int    `json:"ttl"`
	Key    string `json:"key" validate:"required,min=1,max=30000"`
	Value  string `json:"value" validate:"required,min=1"`
}

type Key struct {
	ApiKey string `json:"api_key"`
	Key    string `json:"key" validate:"required,min=1,max=30000"`
}

type Value struct {
	Found bool   `json:"found"`
	Value string `json:"value"`
}

type DeleteDB struct {
	Name string `json:"name" validate:"required,min=1,max=100,alphanum"`
}

type OK struct {
	OK bool `json:"ok"`
}
