// Package match implements the simple glob matching used by routing rules to
// compare namespaces and metadata values.
//
// A pattern is one of four forms: a literal (exact match), a prefix glob
// ("foo*", starts-with), a suffix glob ("*foo", ends-with), or a contains glob
// ("*foo*"). A pattern consisting only of '*' (for example "*", "**") matches
// anything. A "*" anywhere other than the leading or trailing position is
// rejected at compile time.
package match
