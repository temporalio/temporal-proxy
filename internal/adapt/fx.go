package adapt

import "go.uber.org/fx"

// Module wires the adapt layer into an fx application: it provides [crypto.CryptoParams]
// built from the app config and starts background DEK rotation via [RotateDEKs].
var Module = fx.Options(
	fx.Provide(CryptoPolicies),
	fx.Invoke(RotateDEKs),
)
