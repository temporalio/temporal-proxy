package main

import (
	"cmp"
	"context"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/auth/v1"
	"github.com/temporalio/temporal-proxy/pkg/ext"
	"github.com/temporalio/temporal-proxy/pkg/logger"
	"github.com/temporalio/temporal-proxy/pkg/logger/tag"
)

const (
	// workflowService prefixes every method in the table below. The proxy sends the
	// full gRPC method name, leading slash included.
	workflowService = "/temporal.api.workflowservice.v1.WorkflowService/"

	// credentialHeader is the header config.yaml declares as carrying the caller's
	// credential, and so the header the proxy lifts into the request and the one
	// ext.BearerToken is asked for.
	credentialHeader = "authorization"
)

const (
	// The scope a method acts in, which decides whose roles are consulted: a
	// namespace-scoped method reads the caller's roles in the namespace it names,
	// while a cluster-scoped one has no namespace and reads only system roles.
	scopeCluster scope = iota + 1
	scopeNamespace
)

const (
	// How much authority a method calls for, which maps to a role in access.role.
	accessReadOnly access = iota + 1
	accessWrite
	accessAdmin
)

var (
	// methods is what each method costs. The scope and access values are Temporal's
	// own, taken from go.temporal.io/server/common/api's method metadata, so a
	// token that works here works against a Temporal Service configured with the
	// default authorizer.
	//
	// It is a subset: it covers what the Go SDK exercises plus a few neighbours
	// that make the model legible. Anything absent is treated as cluster-scoped
	// admin by Authenticate, so the gap costs a caller access rather than granting
	// it. Temporal derives this for every method instead of listing them; see the
	// README.
	methods = map[string]rule{
		// Namespace scope, read only. A reader can watch, and nothing more.
		workflowService + "DescribeNamespace":                  {scopeNamespace, accessReadOnly},
		workflowService + "DescribeTaskQueue":                  {scopeNamespace, accessReadOnly},
		workflowService + "DescribeWorkflowExecution":          {scopeNamespace, accessReadOnly},
		workflowService + "GetWorkflowExecutionHistory":        {scopeNamespace, accessReadOnly},
		workflowService + "GetWorkflowExecutionHistoryReverse": {scopeNamespace, accessReadOnly},
		workflowService + "ListWorkflowExecutions":             {scopeNamespace, accessReadOnly},
		workflowService + "PollWorkflowExecutionUpdate":        {scopeNamespace, accessReadOnly},
		workflowService + "QueryWorkflow":                      {scopeNamespace, accessReadOnly},

		// Namespace scope, write. Both starting work and serving it live here, which
		// is why a worker needs writer rather than the worker role; see the README.
		workflowService + "DeleteWorkflowExecution":          {scopeNamespace, accessWrite},
		workflowService + "PollActivityTaskQueue":            {scopeNamespace, accessWrite},
		workflowService + "PollWorkflowTaskQueue":            {scopeNamespace, accessWrite},
		workflowService + "RecordActivityTaskHeartbeat":      {scopeNamespace, accessWrite},
		workflowService + "RecordWorkerHeartbeat":            {scopeNamespace, accessWrite},
		workflowService + "ResetWorkflowExecution":           {scopeNamespace, accessWrite},
		workflowService + "RespondActivityTaskCanceled":      {scopeNamespace, accessWrite},
		workflowService + "RespondActivityTaskCompleted":     {scopeNamespace, accessWrite},
		workflowService + "RespondActivityTaskFailed":        {scopeNamespace, accessWrite},
		workflowService + "RespondWorkflowTaskCompleted":     {scopeNamespace, accessWrite},
		workflowService + "RespondWorkflowTaskFailed":        {scopeNamespace, accessWrite},
		workflowService + "ShutdownWorker":                   {scopeNamespace, accessWrite},
		workflowService + "SignalWithStartWorkflowExecution": {scopeNamespace, accessWrite},
		workflowService + "SignalWorkflowExecution":          {scopeNamespace, accessWrite},
		workflowService + "StartWorkflowExecution":           {scopeNamespace, accessWrite},
		workflowService + "TerminateWorkflowExecution":       {scopeNamespace, accessWrite},
		workflowService + "UpdateWorkflowExecution":          {scopeNamespace, accessWrite},

		// Namespace scope, admin. Creating a namespace is namespace-scoped even
		// though the namespace does not exist yet.
		workflowService + "RegisterNamespace": {scopeNamespace, accessAdmin},

		// Cluster scope. Nothing here names a namespace, so only system roles count.
		// GetSystemInfo would belong here too, but alwaysAllowed answers it first.
		workflowService + "GetClusterInfo": {scopeCluster, accessReadOnly},
		workflowService + "ListNamespaces": {scopeCluster, accessReadOnly},
	}
)

type (
	// scope is whether a method acts on a namespace or on the cluster.
	scope int

	// access is how much authority a method calls for.
	access int

	// rule is what the table records for one method.
	rule struct {
		scope  scope
		access access
	}

	// authorizer decides whether an inbound caller may proceed, and is this
	// example's equivalent of a Temporal Authorizer. It satisfies ext.Auth, so
	// pkg/ext serves it as api.auth.v1.AuthService.
	//
	// Both halves of the decision live here: the mapper turns the caller's
	// credential into claims, and Authenticate weighs those claims against what the
	// call is addressing. Temporal splits that across a ClaimMapper and an
	// Authorizer; the proxy asks once, so one RPC does both.
	authorizer struct {
		mapper *mapper
		log    logger.Logger
	}
)

// newAuthorizer returns an authorizer mapping claims with m.
func newAuthorizer(m *mapper, log logger.Logger) *authorizer {
	return &authorizer{mapper: m, log: log}
}

// where names the place a role is held, for a deny reason. A namespace-scoped
// method holds within the namespace it names, while a cluster-scoped one has no
// namespace to name and so would otherwise read as denied in namespace "(none)".
func (s scope) where(ns string) string {
	if s == scopeNamespace {
		return fmt.Sprintf("in namespace %s", namespaceFor(ns))
	}

	return "at cluster scope"
}

// role returns the role a caller must hold to be granted this access. Anything
// beyond read and write is admin, so an access value added later is the most
// expensive one rather than the cheapest.
func (a access) role() role {
	switch a {
	case accessReadOnly:
		return roleReader
	case accessWrite:
		return roleWriter
	default:
		return roleAdmin
	}
}

// Authenticate decides whether the caller behind an inbound stream may proceed.
//
// The two kinds of failure answer differently, and the difference is the point.
// A credential this server could not turn into claims is an authentication
// failure, reported as an error so its Unauthenticated code reaches the caller
// and tells it to get a new token. Claims that were mapped and then found
// wanting are an authorization failure, reported as DECISION_DENY, which the
// proxy turns into PermissionDenied: the token is fine, and another one like it
// will not help.
//
// A deny carries a reason naming the subject, what it holds, and what the call
// wanted. The proxy logs that and withholds it from the caller, so it can say
// more than a refused caller should be told.
func (a *authorizer) Authenticate(_ context.Context, req *auth.AuthRequest) (*auth.AuthResponse, error) {
	target := req.GetTarget()
	full, ns := target.GetFullName(), target.GetNamespace()

	// Answered before the credential is read at all, which is what Temporal's own
	// authorizer does with the same set. Allowing them is this server's choice, not
	// something pkg/ext decided: ext.IsHealthCheckMethod only reports membership.
	// GetSystemInfo is in there, so a caller with no token still dials successfully
	// and is refused on its first real call instead.
	if ext.IsHealthCheckMethod(full) {
		a.log.Info("Allowed without reading a credential", tag.String("method", full))

		return ext.Allow(), nil
	}

	// Already an Unauthenticated status error, so it is returned as it is.
	bearer, err := ext.BearerToken(req, credentialHeader)
	if err != nil {
		a.logRejection(full, status.Convert(err).Message())

		return nil, err
	}

	cl, err := a.mapper.claimsFrom(bearer)
	if err != nil {
		a.logRejection(full, err.Error())

		return nil, status.Error(codes.Unauthenticated, err.Error())
	}

	r, ok := methods[full]
	if !ok {
		// Fail closed. A method the table does not describe is charged as the most
		// privileged thing it could be, so forgetting an entry denies a caller
		// rather than waving it through.
		r = rule{scope: scopeCluster, access: accessAdmin}
	}

	// System roles apply everywhere, so they are the floor in either scope. An
	// empty namespace means the proxy resolved none, not a namespace named "": the
	// lookup misses, and only system roles count.
	have := cl.system
	if r.scope == scopeNamespace {
		have |= cl.namespaces[ns]
	}

	// The comparison Temporal's default authorizer makes, kept as it is: an
	// ordering test on a bitmask rather than a mask test. It is why the worker role
	// grants nothing on its own, since it numbers below reader. The README says so
	// rather than quietly improving on it.
	need := r.access.role()
	if have >= need {
		a.log.Info(
			"Allowed",
			tag.String("subject", cl.subject),
			tag.String("method", full),
			tag.String("namespace", namespaceFor(ns)),
			tag.Stringer("holds", have),
		)

		return ext.Allow(), nil
	}

	reason := fmt.Sprintf(
		"subject %q holds %s %s, and %s calls for %s",
		cl.subject, have, r.scope.where(ns), full, need,
	)

	a.log.Warn(
		"Denied",
		tag.String("subject", cl.subject),
		tag.String("method", full),
		tag.String("namespace", namespaceFor(ns)),
		tag.Stringer("holds", have),
		tag.Stringer("needs", need),
	)

	return ext.Deny(reason), nil
}

// logRejection records a credential this server could not turn into claims. The
// caller is told only Unauthenticated, so this log line is where the detail
// survives.
func (a *authorizer) logRejection(full, reason string) {
	a.log.Warn(
		"Rejecting credential",
		tag.String("method", full),
		tag.String("reason", reason),
	)
}

// namespaceFor renders a target namespace for a log line or a deny reason. An
// empty one is spelled out, since it means the proxy had no namespace to resolve
// rather than a namespace whose name is empty.
func namespaceFor(ns string) string {
	return cmp.Or(ns, "(none)")
}
