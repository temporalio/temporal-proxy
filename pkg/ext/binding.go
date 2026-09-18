package ext

import (
	"encoding/binary"
	"fmt"
	"math"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/temporalio/temporal-proxy/pkg/api/ext/v1"
)

// BindingContext returns the bytes binding key material to the fields that
// travel beside it in the clear, for a [KeySealer] to hand its key service as
// an encryption context. Material framed by [NewSealWrapper] names no cipher,
// so there is none to pass here: a sealer that wants to record which
// construction it used puts that in Opaque, which this binds.
func BindingContext(namespace, version string, opaque []byte) ([]byte, error) {
	return bindingContext(ext.KeyMaterial_CIPHER_UNSPECIFIED, namespace, version, opaque)
}

// additionalData returns the AEAD additional data binding every field of km that
// travels in the clear, so none of them can be swapped for another's: material
// relabelled with a different namespace, version, or cipher fails to open rather
// than opening under the wrong assumption.
func additionalData(km *ext.KeyMaterial) ([]byte, error) {
	return bindingContext(km.GetCipher(), km.GetNamespace(), km.GetVersion(), km.GetOpaque())
}

// bindingContext is the encoding behind [BindingContext] and [additionalData].
// It is length-prefixed so that no two different sets of fields produce the same
// bytes, and it never travels anywhere. Both ends recompute it.
//
// It is permanent: every payload already sealed authenticates against it, so it
// can be added to only at the end, and only alongside a new cipher id. The
// emission order below is fixed forever and is deliberately not the parameter
// order, so read it rather than the signature before touching anything.
func bindingContext(c CipherID, namespace, version string, opaque []byte) ([]byte, error) {
	if len(version) > math.MaxUint16 {
		return nil, fmt.Errorf("key version is too long: %d bytes", len(version))
	}

	if len(namespace) > math.MaxUint16 {
		return nil, status.Errorf(codes.InvalidArgument, "namespace is too long: %d bytes", len(namespace))
	}

	if len(opaque) > math.MaxUint32 {
		return nil, fmt.Errorf("opaque is too long: %d bytes", len(opaque))
	}

	ad := make([]byte, 0, 12+len(version)+len(namespace)+len(opaque))
	ad = binary.BigEndian.AppendUint32(ad, uint32(c))
	ad = binary.BigEndian.AppendUint16(ad, uint16(len(version)))
	ad = append(ad, version...)
	ad = binary.BigEndian.AppendUint16(ad, uint16(len(namespace)))
	ad = append(ad, namespace...)
	ad = binary.BigEndian.AppendUint32(ad, uint32(len(opaque)))
	ad = append(ad, opaque...)

	return ad, nil
}
