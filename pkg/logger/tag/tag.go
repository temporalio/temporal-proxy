package tag

import "fmt"

type (
	// Tag is a single structured key/value pair attached to a log entry.
	Tag struct {
		Key   string
		Value any
	}
)

// Component returns a tag with key "component" and the supplied value.
func Component(c string) Tag {
	return String("component", c)
}

// Error returns a Tag with key "error" carrying err's message.
func Error(err error) Tag {
	msg := ""
	if err != nil {
		msg = err.Error()
	}

	return String("error", msg)
}

// String returns a Tag for the given key and string value.
func String(k, v string) Tag {
	return Tag{Key: k, Value: v}
}

// Stringer returns a Tag whose value is v.String(), evaluated immediately.
func Stringer(k string, v fmt.Stringer) Tag {
	return String(k, v.String())
}
