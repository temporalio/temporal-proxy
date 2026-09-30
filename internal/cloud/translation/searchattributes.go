package translation

import (
	enumspb "go.temporal.io/api/enums/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	cloudnamespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"go.temporal.io/cloud-sdk/cloudclient"

	"github.com/temporalio/temporal-proxy/internal/services"
)

const (
	// listSearchAttributesMethod is the OperatorService method that lists a
	// namespace's search attributes. Cloud frontends refuse every OperatorService
	// call from a customer credential, whatever its role, so it is answered from
	// the namespace's Cloud spec instead.
	listSearchAttributesMethod = "/" + services.OperatorService + "/ListSearchAttributes"
	getNamespaceMethod         = "/" + cloudService + "/GetNamespace"
)

// listSearchAttributes translates OperatorService.ListSearchAttributes onto
// CloudService.GetNamespace, pinned to the Cloud API version for the same reason
// as listNamespaces. It carries a namespace, so it fires on whichever Cloud
// upstream that namespace routes to.
func listSearchAttributes() *Translation {
	return Adapt(listSearchAttributesMethod, getNamespaceMethod, listSearchAttributesRequest, listSearchAttributesResponse).
		WithHeader(cloudclient.TemporalCloudAPIVersionHeader(), cloudclient.DefaultAPIVersion())
}

// listSearchAttributesRequest converts a ListSearchAttributes request into the
// GetNamespace request for the same namespace. The name is already the remote
// one, since namespace translation runs outside method translation.
func listSearchAttributesRequest(req *operatorservice.ListSearchAttributesRequest) (*cloudservice.GetNamespaceRequest, error) {
	return &cloudservice.GetNamespaceRequest{Namespace: req.GetNamespace()}, nil
}

// listSearchAttributesResponse reports the namespace's custom search attributes
// from its Cloud spec. System attributes and the storage schema are left empty:
// Cloud reports neither, and a hand-kept list of Temporal's built-ins would be a
// claim about what Cloud indexes that Cloud never made.
func listSearchAttributesResponse(
	_ *operatorservice.ListSearchAttributesRequest,
	up *cloudservice.GetNamespaceResponse,
	reply *operatorservice.ListSearchAttributesResponse,
) error {
	attrs := up.GetNamespace().GetSpec().GetSearchAttributes()

	custom := make(map[string]enumspb.IndexedValueType, len(attrs))
	for name, typ := range attrs {
		custom[name] = indexedValueType(typ)
	}

	reply.Reset()
	reply.CustomAttributes = custom

	return nil
}

// indexedValueType maps the type Cloud reports for a search attribute onto the
// indexed value type a Temporal Service reports. An unrecognized type stays
// unspecified rather than being guessed at.
func indexedValueType(typ cloudnamespace.NamespaceSpec_SearchAttributeType) enumspb.IndexedValueType {
	switch typ {
	case cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_TEXT:
		return enumspb.INDEXED_VALUE_TYPE_TEXT
	case cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD:
		return enumspb.INDEXED_VALUE_TYPE_KEYWORD
	case cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_INT:
		return enumspb.INDEXED_VALUE_TYPE_INT
	case cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DOUBLE:
		return enumspb.INDEXED_VALUE_TYPE_DOUBLE
	case cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_BOOL:
		return enumspb.INDEXED_VALUE_TYPE_BOOL
	case cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DATETIME:
		return enumspb.INDEXED_VALUE_TYPE_DATETIME
	case cloudnamespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST:
		return enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST
	default:
		return enumspb.INDEXED_VALUE_TYPE_UNSPECIFIED
	}
}
