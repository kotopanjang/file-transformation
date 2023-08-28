package exception

import (
	"fmt"
)

// Exceptions.
var (
	ErrUnauthorized        error = fmt.Errorf("unauthorized")
	ErrNotFound            error = fmt.Errorf("not found")
	ErrInternalServer      error = fmt.Errorf("internal server error")
	ErrConflict            error = fmt.Errorf("conflicted")
	ErrUnprocessableEntity error = fmt.Errorf("unprocessable entity")
	ErrBadRequest          error = fmt.Errorf("bad request")
	ErrGatewayTimeout      error = fmt.Errorf("gateway timeout")
	ErrTimeout             error = fmt.Errorf("request time out")
	ErrLocked              error = fmt.Errorf("locked")
	ErrForbidden           error = fmt.Errorf("forbidden")
	ErrNotImplemented      error = fmt.Errorf("not Implemented")
)
