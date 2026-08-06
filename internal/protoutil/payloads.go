package protoutil

import "google.golang.org/protobuf/reflect/protoreflect"

// payloadType is the message every encryptable value is wrapped in. A message
// type that cannot reach it carries nothing encryption would touch.
const payloadType protoreflect.FullName = "temporal.api.common.v1.Payload"

// CarriesPayloads reports whether md transitively reaches a
// temporal.api.common.v1.Payload field, following singular, repeated, and
// map-valued message fields.
//
// It answers whether a message is worth encrypting at all, which is what makes
// the startup coverage check precise: reflection messages reach no payload and
// are safe to forward with encryption on, while a service whose messages do
// reach one and that the payload visitor cannot see must be refused rather than
// forwarded in cleartext.
func CarriesPayloads(md protoreflect.MessageDescriptor) bool {
	return carriesPayloads(md, map[protoreflect.FullName]bool{})
}

// carriesPayloads walks md depth first, marking each type in seen before
// walking its fields so a cyclic type graph terminates. Marking early can only
// produce a stale false, never a stale true: a node revisited through a
// still-pending ancestor sits on that ancestor's own path to Payload, and a
// simple path either reaches Payload through a later sibling field first (so
// the ancestor resolves true and the stale false never surfaces) or loops back
// through the pending node, which a simple path cannot do.
func carriesPayloads(md protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
	if md.FullName() == payloadType {
		return true
	}

	if seen[md.FullName()] {
		return false
	}
	seen[md.FullName()] = true

	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		if sub := childMessage(fields.Get(i)); sub != nil && carriesPayloads(sub, seen) {
			return true
		}
	}

	return false
}
