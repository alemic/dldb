package server

/*
	net.Error
	type Error interface {
	error
	Timeout() bool   // Is the error a timeout?
	Temporary() bool // Is the error temporary?
}
*/
//compatible with net.Error
type InvalidRequestError string

func (self InvalidRequestError) Error() string { return "Invalid request, " + string(self) }

func (self InvalidRequestError) Temporary() bool { return true }

func (self InvalidRequestError) Timeout() bool { return false }
