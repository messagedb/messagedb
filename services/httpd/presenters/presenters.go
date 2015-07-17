package presenters

import (
	"net/url"
)

type Map map[string]interface{}

type Presentable interface {
	ToPresenterMap() Map
}

type Locationable interface {
	GetLocation() *url.URL
}
