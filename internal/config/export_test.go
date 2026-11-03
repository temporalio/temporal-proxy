package config

// NamespaceUpstream exposes namespaceUpstream to the guard that pins it against
// the dataplane's own choice.
var NamespaceUpstream = (*Routing).namespaceUpstream
