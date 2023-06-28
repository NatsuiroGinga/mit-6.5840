package models

type Operation uint8

const (
	GET Operation = iota + 1
	PUT
	APPEND
)

func (this Operation) String() string {
	switch this {
	case GET:
		return "GET"
	case PUT:
		return "PUT"
	case APPEND:
		return "APPEND"
	default:
		return "UNKNOWN"
	}
}
