package translation

import (
	nexuspb "go.temporal.io/api/nexus/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnexus "go.temporal.io/cloud-sdk/api/nexus/v1"
	"go.temporal.io/cloud-sdk/api/resource/v1"
	"go.temporal.io/cloud-sdk/cloudclient"

	"github.com/temporalio/temporal-proxy/internal/services"
)

const (
	// listNexusEndpointsMethod and getNexusEndpointMethod are the OperatorService
	// methods that read Nexus endpoints. They carry no namespace, and Cloud
	// frontends refuse every OperatorService call from a customer credential, so
	// they are answered by the CloudService methods below instead.
	listNexusEndpointsMethod = "/" + services.OperatorService + "/ListNexusEndpoints"
	getNexusEndpointMethod   = "/" + services.OperatorService + "/GetNexusEndpoint"

	getNexusEndpointsMethod     = "/" + cloudService + "/GetNexusEndpoints"
	getCloudNexusEndpointMethod = "/" + cloudService + "/GetNexusEndpoint"
)

// listNexusEndpoints translates OperatorService.ListNexusEndpoints onto
// CloudService.GetNexusEndpoints, pinned to the Cloud API version for the same
// reason as listNamespaces.
func listNexusEndpoints() *Translation {
	return Adapt(
		listNexusEndpointsMethod,
		getNexusEndpointsMethod,
		listNexusEndpointsRequest,
		listNexusEndpointsResponse,
	).WithHeader(cloudclient.TemporalCloudAPIVersionHeader(), cloudclient.DefaultAPIVersion())
}

// getNexusEndpoint translates OperatorService.GetNexusEndpoint onto
// CloudService.GetNexusEndpoint.
func getNexusEndpoint() *Translation {
	return Adapt(
		getNexusEndpointMethod,
		getCloudNexusEndpointMethod,
		getNexusEndpointRequest,
		getNexusEndpointResponse,
	).WithHeader(cloudclient.TemporalCloudAPIVersionHeader(), cloudclient.DefaultAPIVersion())
}

// listNexusEndpointsRequest converts a ListNexusEndpoints request into the
// GetNexusEndpoints request that stands in for it. The page token is carried
// across verbatim, as for ListNamespaces, and the name filter has a direct
// equivalent.
func listNexusEndpointsRequest(req *operatorservice.ListNexusEndpointsRequest) (*cloudservice.GetNexusEndpointsRequest, error) {
	return &cloudservice.GetNexusEndpointsRequest{
		PageSize:  min(req.GetPageSize(), cloudPageLimit),
		PageToken: string(req.GetNextPageToken()),
		Name:      req.GetName(),
	}, nil
}

// listNexusEndpointsResponse folds a GetNexusEndpoints response into the
// ListNexusEndpoints response the caller is waiting on. A deleted endpoint is
// dropped, since a Temporal Service never lists one.
func listNexusEndpointsResponse(
	_ *operatorservice.ListNexusEndpointsRequest,
	up *cloudservice.GetNexusEndpointsResponse,
	reply *operatorservice.ListNexusEndpointsResponse,
) error {
	endpoints := make([]*nexuspb.Endpoint, 0, len(up.GetEndpoints()))
	for _, ep := range up.GetEndpoints() {
		if ep.GetState() == resource.ResourceState_RESOURCE_STATE_DELETED {
			continue
		}

		endpoints = append(endpoints, nexusEndpoint(ep))
	}

	reply.Endpoints = endpoints
	reply.NextPageToken = []byte(up.GetNextPageToken())

	return nil
}

// getNexusEndpointRequest converts a GetNexusEndpoint request into the Cloud
// request for the same endpoint id.
func getNexusEndpointRequest(req *operatorservice.GetNexusEndpointRequest) (*cloudservice.GetNexusEndpointRequest, error) {
	return &cloudservice.GetNexusEndpointRequest{EndpointId: req.GetId()}, nil
}

// getNexusEndpointResponse folds the Cloud endpoint into the caller's reply.
func getNexusEndpointResponse(
	_ *operatorservice.GetNexusEndpointRequest,
	up *cloudservice.GetNexusEndpointResponse,
	reply *operatorservice.GetNexusEndpointResponse,
) error {
	reply.Endpoint = nexusEndpoint(up.GetEndpoint())
	return nil
}

// nexusEndpoint converts one Cloud Nexus endpoint into the endpoint a Temporal
// Service reports. Cloud's worker target names its namespace by id, which on
// Cloud is the fully qualified name a client addresses it by. Version is left
// zero, since Cloud's resource version is an opaque string rather than a
// counter, and UrlPrefix is left empty rather than naming a path Cloud does not
// serve.
func nexusEndpoint(ep *cloudnexus.Endpoint) *nexuspb.Endpoint {
	spec := ep.GetSpec()

	out := &nexuspb.Endpoint{
		Id:               ep.GetId(),
		CreatedTime:      ep.GetCreatedTime(),
		LastModifiedTime: ep.GetLastModifiedTime(),
		Spec: &nexuspb.EndpointSpec{
			Name:        spec.GetName(),
			Description: spec.GetDescription(),
		},
	}

	if w := spec.GetTargetSpec().GetWorkerTargetSpec(); w != nil {
		out.Spec.Target = &nexuspb.EndpointTarget{
			Variant: &nexuspb.EndpointTarget_Worker_{
				Worker: &nexuspb.EndpointTarget_Worker{Namespace: w.GetNamespaceId(), TaskQueue: w.GetTaskQueue()},
			},
		}
	}

	return out
}
