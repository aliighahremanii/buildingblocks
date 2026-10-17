// Package i18n provides MultilingualString, a value object for the recurring
// legacy pattern of paired Farsi/English name columns (e.g. CountryName /
// EnCountryName, NameFa / NameEn).
//
// The mapping convention used throughout the codebase is:
//
//   - Local()  -> Farsi column  (NameFa / CountryName / CityName)
//   - Intl()   -> English column (NameEn / EnCountryName / EnCityName)
//
// The legacy NestJS codebase uses valueLocal/valueIntl; this Go port keeps the
// same semantics under the shorter Local/Intl method names.
package i18n

import (
	"errors"
	"strings"
)

var (
	ErrNameRequired = errors.New("at least one of local or intl name is required")
)

// MultilingualString is an immutable two-language name.
type MultilingualString struct {
	local string
	intl  string
}

// New constructs a MultilingualString from trimmed inputs. At least one of the
// two values must be non-empty after trimming.
func New(local, intl string) (MultilingualString, error) {
	local = strings.TrimSpace(local)
	intl = strings.TrimSpace(intl)
	if local == "" && intl == "" {
		return MultilingualString{}, ErrNameRequired
	}
	return MultilingualString{local: local, intl: intl}, nil
}

// Rehydrate constructs a MultilingualString without validation. Use only when
// reading from a persisted row, where empty strings must round-trip faithfully.
func Rehydrate(local, intl string) MultilingualString {
	return MultilingualString{local: local, intl: intl}
}

// Local returns the Farsi / primary-language value.
func (m MultilingualString) Local() string { return m.local }

// Intl returns the English / international value.
func (m MultilingualString) Intl() string { return m.intl }

// Equal reports whether two MultilingualString values are identical.
func (m MultilingualString) Equal(other MultilingualString) bool {
	return m.local == other.local && m.intl == other.intl
}

// IsEmpty reports whether both language slots are empty. A valid
// MultilingualString constructed via New is never empty; this is useful when
// filtering Rehydrate results.
func (m MultilingualString) IsEmpty() bool {
	return m.local == "" && m.intl == ""
}
