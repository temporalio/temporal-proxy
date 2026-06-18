// Package visitcheck provides a protoreflect-based oracle used by generated
// field-visitor tests. It populates messages and independently collects target
// fields so a generated visitor can be checked for completeness by comparing
// pointer identities. It is intended for use from tests only.
package visitcheck

import (
	"reflect"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Collect walks msg recursively via protoreflect and returns every nested
// message whose descriptor full name equals target. The returned values wrap
// the live pointers stored in msg, so callers may compare them by identity
// (see Addrs) against a visitor's collected pointers. A matched target is
// treated as a leaf: Collect does not descend into it, mirroring a generated
// visitor that invokes its callback on the match and stops.
func Collect(msg proto.Message, target protoreflect.FullName) []proto.Message {
	var out []proto.Message
	var walk func(m protoreflect.Message)

	consider := func(rm protoreflect.Message) {
		if rm.Descriptor().FullName() == target {
			out = append(out, rm.Interface())
			return
		}

		walk(rm)
	}

	walk = func(m protoreflect.Message) {
		m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
			switch {
			case fd.IsMap():
				if !isMessageKind(fd.MapValue().Kind()) {
					return true
				}

				v.Map().Range(func(_ protoreflect.MapKey, mv protoreflect.Value) bool {
					consider(mv.Message())
					return true
				})
			case fd.IsList():
				if !isMessageKind(fd.Kind()) {
					return true
				}

				l := v.List()
				for i := 0; i < l.Len(); i++ {
					consider(l.Get(i).Message())
				}
			case isMessageKind(fd.Kind()):
				consider(v.Message())
			}

			return true
		})
	}

	consider(msg.ProtoReflect())
	return out
}

// Addrs returns the pointer address of each element so two slices of pointers
// can be compared by identity (and multiplicity) rather than by value. Each
// element must be a pointer or interface value; non-pointer element types will
// cause reflect.Value.Pointer to panic at runtime.
func Addrs[T any](xs []T) []uintptr {
	out := make([]uintptr, len(xs))
	for i, x := range xs {
		out[i] = reflect.ValueOf(x).Pointer()
	}

	return out
}

func isMessageKind(k protoreflect.Kind) bool {
	return k == protoreflect.MessageKind || k == protoreflect.GroupKind
}

// Variants returns one or more fully populated clones of seed. Every message
// field is set non-nil and every scalar receives a sentinel value. Across the
// returned set, every reachable oneof case is the selected case in at least one
// variant. Population stops descending into a message whose descriptor already
// appears on the current path, which bounds recursive/cyclic types.
func Variants(seed proto.Message) []proto.Message {
	count := variantCount(seed.ProtoReflect().Descriptor())
	out := make([]proto.Message, 0, count)
	for i := range count {
		m := seed.ProtoReflect().New()
		populate(m, i, map[protoreflect.FullName]bool{})
		out = append(out, m.Interface())
	}

	return out
}

// variantCount returns the largest oneof case count among reachable message
// descriptors (at least 1). This is enough to cover every case of every oneof:
// for variant indices 0..maxCases-1, `i % n` ranges over all residues 0..n-1
// whenever maxCases >= n, which holds for every oneof since maxCases is the
// maximum.
func variantCount(root protoreflect.MessageDescriptor) int {
	maxCases := 1
	seen := map[protoreflect.FullName]bool{}

	var walk func(md protoreflect.MessageDescriptor)
	walk = func(md protoreflect.MessageDescriptor) {
		if seen[md.FullName()] {
			return
		}

		seen[md.FullName()] = true
		for i := 0; i < md.Oneofs().Len(); i++ {
			if n := md.Oneofs().Get(i).Fields().Len(); n > maxCases {
				maxCases = n
			}
		}

		for i := 0; i < md.Fields().Len(); i++ {
			fd := md.Fields().Get(i)
			if isMessageKind(fd.Kind()) {
				walk(fd.Message())
			}
		}
	}

	walk(root)
	return maxCases
}

func populate(m protoreflect.Message, variant int, path map[protoreflect.FullName]bool) {
	md := m.Descriptor()
	if path[md.FullName()] {
		return // cycle: leave fields unset to terminate
	}
	path[md.FullName()] = true
	defer delete(path, md.FullName())

	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if fd.ContainingOneof() != nil {
			continue // oneofs handled below so we set exactly one case
		}
		setField(m, fd, variant, path)
	}

	for i := 0; i < md.Oneofs().Len(); i++ {
		cases := md.Oneofs().Get(i).Fields()
		setField(m, cases.Get(variant%cases.Len()), variant, path)
	}
}

func setField(m protoreflect.Message, fd protoreflect.FieldDescriptor, variant int, path map[protoreflect.FullName]bool) {
	switch {
	case fd.IsMap():
		mp := m.Mutable(fd).Map()
		key := mapKeySentinel(fd.MapKey())
		if isMessageKind(fd.MapValue().Kind()) {
			val := mp.NewValue()
			populate(val.Message(), variant, path)
			mp.Set(key, val)
		} else {
			mp.Set(key, scalarSentinel(fd.MapValue()))
		}
	case fd.IsList():
		lst := m.Mutable(fd).List()
		if isMessageKind(fd.Kind()) {
			ev := lst.NewElement()
			populate(ev.Message(), variant, path)
			lst.Append(ev)
		} else {
			lst.Append(scalarSentinel(fd))
		}
	case isMessageKind(fd.Kind()):
		populate(m.Mutable(fd).Message(), variant, path)
	default:
		m.Set(fd, scalarSentinel(fd))
	}
}

func scalarSentinel(fd protoreflect.FieldDescriptor) protoreflect.Value {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(1)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(1)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(1)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(1)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("x")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{1})
	case protoreflect.EnumKind:
		// Use the second declared enum value when present (index 0 is the proto3
		// zero value); fall back to the zero value otherwise.
		vals := fd.Enum().Values()
		if vals.Len() > 1 {
			return protoreflect.ValueOfEnum(vals.Get(1).Number())
		}
		return protoreflect.ValueOfEnum(vals.Get(0).Number())
	default:
		return fd.Default()
	}
}

func mapKeySentinel(fd protoreflect.FieldDescriptor) protoreflect.MapKey {
	return scalarSentinel(fd).MapKey()
}
