package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewAuthorizer creates a new Authorizer instance, wrapping around a Casbin's enforce instance
func NewAuthorizer(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

// Authorizer wrapps around Casbin's enforcer
type Authorizer struct {
	enforcer *casbin.Enforcer
}

// Authorize defer Casbin's Enforce function
func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf("%s is not permitted to %s to %s", subject, action, object)
		st := status.New(codes.PermissionDenied, msg)

		return st.Err()
	}
	return nil
}
