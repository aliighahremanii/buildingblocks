package webhttp

import (
	"net/url"
	"os"
	"strings"
)

const (
	httpPathPrefixEnv         = "HTTP_PATH_PREFIX"
	httpAPIBasePathEnv        = "HTTP_API_BASE_PATH"
	httpAPIVersionEnv         = "HTTP_API_VERSION"
	defaultHTTPAPIBasePath    = "/api"
	defaultHTTPAPIVersion     = "v1"
	modulePathPrefixEnvSuffix = "_HTTP_PATH_PREFIX"
)

func PathPrefixFromEnv() string {
	return PathPrefixForModule("")
}

func PathPrefixForModule(moduleSegment string) string {
	if raw, ok := lookupPathPrefixEnv(modulePathPrefixEnvKey(moduleSegment)); ok {
		return NormalizePathPrefix(raw)
	}
	if raw, ok := lookupPathPrefixEnv(httpPathPrefixEnv); ok {
		return NormalizePathPrefix(raw)
	}
	return DefaultPathPrefixForModule(moduleSegment)
}

func DefaultPathPrefixForModule(moduleSegment string) string {
	moduleSegment = normalizeModuleSegment(moduleSegment)
	basePath := normalizeBasePath(os.Getenv(httpAPIBasePathEnv))
	version := normalizeAPIVersion(os.Getenv(httpAPIVersionEnv))

	prefix := basePath
	if moduleSegment != "" {
		prefix = JoinPath(prefix, moduleSegment)
	}
	if version == "" {
		return prefix
	}
	return JoinPath(prefix, version)
}

func NormalizePathPrefix(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "/"
	}

	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}

	if raw == "/" {
		return "/"
	}

	return strings.TrimRight(raw, "/")
}

func JoinPath(prefix, path string) string {
	prefix = NormalizePathPrefix(prefix)
	if prefix == "/" {
		return path
	}

	if path == "" {
		return prefix
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	return prefix + path
}

func JoinURLPath(baseURL, pathPrefix string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	pathPrefix = NormalizePathPrefix(pathPrefix)
	if pathPrefix == "/" {
		return baseURL
	}

	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		if strings.HasSuffix(baseURL, pathPrefix) {
			return baseURL
		}
		return baseURL + pathPrefix
	}

	existingPath := strings.TrimRight(parsed.Path, "/")
	if existingPath == pathPrefix {
		return parsed.String()
	}
	if existingPath == "" {
		parsed.Path = pathPrefix
	} else {
		parsed.Path = existingPath + pathPrefix
	}

	return parsed.String()
}

func ServiceBaseURLFromEnv(envKey, defaultHost string) string {
	return ServiceBaseURLFromEnvForModule(envKey, defaultHost, "")
}

func ServiceBaseURLFromEnvForModule(envKey, defaultHost, moduleSegment string) string {
	baseURL := os.Getenv(envKey)
	if baseURL == "" {
		baseURL = defaultHost
	}
	return JoinURLPath(baseURL, PathPrefixForModule(moduleSegment))
}

func lookupPathPrefixEnv(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	raw, ok := os.LookupEnv(key)
	if !ok {
		return "", false
	}
	if strings.TrimSpace(raw) == "" {
		return "", false
	}
	return raw, true
}

func modulePathPrefixEnvKey(moduleSegment string) string {
	moduleSegment = normalizeModuleSegment(moduleSegment)
	if moduleSegment == "" {
		return ""
	}
	replacer := strings.NewReplacer("-", "_", "/", "_")
	return strings.ToUpper(replacer.Replace(moduleSegment)) + modulePathPrefixEnvSuffix
}

func normalizeModuleSegment(moduleSegment string) string {
	return strings.Trim(strings.TrimSpace(moduleSegment), "/")
}

func normalizeBasePath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultHTTPAPIBasePath
	}
	return NormalizePathPrefix(raw)
}

func normalizeAPIVersion(raw string) string {
	raw = strings.Trim(strings.TrimSpace(raw), "/")
	if raw == "" {
		return defaultHTTPAPIVersion
	}
	return raw
}
