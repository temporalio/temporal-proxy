package protoutil

import (
	"net/url"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

type (
	// fieldWriter is one field of a message type that carries a namespace, and
	// how to rewrite the namespace inside its value. Each kind of value has its
	// own implementation, so a new encoding is a new type plus an entry in
	// [fieldWriters].
	fieldWriter interface {
		// Rewrite maps every namespace the field holds in m through fn. An unset
		// or empty value is left alone, so an unset namespace is not wrapped.
		Rewrite(m protoreflect.Message, fn func(string) string)
		// Field is the field this writer rewrites.
		Field() protoreflect.FieldDescriptor
	}

	// newFieldWriter builds the fieldWriter for one field.
	newFieldWriter func(protoreflect.FieldDescriptor) fieldWriter

	// nameField is a string field, or repeated string, whose value is a
	// namespace name.
	nameField struct{ fd protoreflect.FieldDescriptor }

	// linkURLField is a string field holding a Temporal link URL, whose
	// namespace is its first path segment (see [translateLinkURL]).
	linkURLField struct{ fd protoreflect.FieldDescriptor }
)

// fieldWriters lists the string fields the suffix rule in [writerFor] does not
// classify, keyed by the fully qualified message type and then the field name,
// with the writer each one takes.
//
//   - NamespaceInfo.name is the namespace name DescribeNamespace and
//     ListNamespaces return, both WorkflowService methods through the proxy.
//   - A Nexus link names the namespace of the Workflow, operation, or callback
//     it points at inside its URL. An SDK converts it to and from a typed
//     common.v1.Link, whose namespace field is translated, so leaving the URL
//     alone would hand that conversion the other side's name.
var fieldWriters = map[protoreflect.FullName]map[protoreflect.Name]newFieldWriter{
	"temporal.api.namespace.v1.NamespaceInfo": {"name": newNameField},
	"temporal.api.nexus.v1.Link":              {"url": newLinkURLField},
}

const (
	// linkURLScheme is the scheme of a Temporal link URL.
	linkURLScheme = "temporal"
	// linkNamespacesPath starts the path of every Temporal link URL, its
	// namespace being the path segment after it.
	linkNamespacesPath = "/namespaces/"
)

// writerFor returns the writer for fd, declared on md, or nil when fd carries no
// namespace. Only string fields qualify. An entry in [fieldWriters] wins;
// otherwise a field whose proto name ends in "namespace" is a namespace name,
// which excludes "*namespace_id", since those end in "_id".
func writerFor(md protoreflect.MessageDescriptor, fd protoreflect.FieldDescriptor) fieldWriter {
	if fd.Kind() != protoreflect.StringKind {
		return nil
	}

	if newWriter, ok := fieldWriters[md.FullName()][fd.Name()]; ok {
		return newWriter(fd)
	}

	if strings.HasSuffix(string(fd.Name()), "namespace") {
		return newNameField(fd)
	}

	return nil
}

func newNameField(fd protoreflect.FieldDescriptor) fieldWriter { return nameField{fd: fd} }

// Rewrite maps the namespace name, or each one of a repeated field, through fn.
func (f nameField) Rewrite(m protoreflect.Message, fn func(string) string) {
	rewriteStrings(m, f.fd, fn)
}

// Field is the namespace-name field.
func (f nameField) Field() protoreflect.FieldDescriptor { return f.fd }

func newLinkURLField(fd protoreflect.FieldDescriptor) fieldWriter { return linkURLField{fd: fd} }

// Rewrite maps the namespace segment of the link URL, or of each one of a
// repeated field, through fn.
func (f linkURLField) Rewrite(m protoreflect.Message, fn func(string) string) {
	rewriteStrings(m, f.fd, func(s string) string { return translateLinkURL(s, fn) })
}

// Field is the link URL field.
func (f linkURLField) Field() protoreflect.FieldDescriptor { return f.fd }

// rewriteStrings replaces each non-empty value of the string field fd in m,
// singular or repeated, with rewrite's result. An absent field is skipped.
func rewriteStrings(m protoreflect.Message, fd protoreflect.FieldDescriptor, rewrite func(string) string) {
	if fd.IsList() {
		list := m.Get(fd).List()
		for i := 0; i < list.Len(); i++ {
			if s := list.Get(i).String(); s != "" {
				list.Set(i, protoreflect.ValueOfString(rewrite(s)))
			}
		}

		return
	}

	if !m.Has(fd) {
		return
	}

	if s := m.Get(fd).String(); s != "" {
		m.Set(fd, protoreflect.ValueOfString(rewrite(s)))
	}
}

// translateLinkURL rewrites the namespace segment of a Temporal link URL with
// fn. It finds the namespace the way the SDK's link converter
// (go.temporal.io/api/temporalnexus) does: it parses s with url.Parse, which
// lowercases the scheme, requires the temporal scheme whatever the host, and
// takes the first path segment after /namespaces/ in the escaped path. Only
// that segment changes: it is unescaped for fn and escaped again, as the
// converter writes it, and the host, the rest of the path, the query, and the
// fragment are kept. Rebuilding the link through the converter instead would
// drop anything it does not model, such as a query parameter a newer SDK
// adds. A URL the converter would not read as a Temporal link, whose namespace
// segment is empty or not a valid escape, or whose name fn leaves unchanged is
// returned as it arrived.
func translateLinkURL(s string, fn func(string) string) string {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != linkURLScheme {
		return s
	}

	rest, ok := strings.CutPrefix(u.EscapedPath(), linkNamespacesPath)
	if !ok {
		return s
	}

	end := strings.IndexByte(rest, '/')
	if end < 0 {
		end = len(rest)
	}

	ns, err := url.PathUnescape(rest[:end])
	if err != nil || ns == "" {
		return s
	}

	// An unchanged name leaves the URL exactly as it arrived, however it was
	// written, so a proxy with no translation rules never alters a link.
	translated := fn(ns)
	if translated == ns {
		return s
	}

	escaped := linkNamespacesPath + url.PathEscape(translated) + rest[end:]
	path, err := url.PathUnescape(escaped)
	if err != nil {
		return s
	}

	u.Path, u.RawPath = path, escaped

	return u.String()
}
