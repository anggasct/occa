package relay

import "strings"

func PermissionRuleIdentity(request PermissionRequest) string {
	permission := strings.TrimSpace(request.Permission)
	if permission != "" {
		if strings.HasPrefix(permission, "call_") {
			return ""
		}
		return permission
	}

	tool := strings.TrimSpace(request.Tool)
	if tool == "" || strings.HasPrefix(tool, "call_") {
		return ""
	}
	return tool
}
