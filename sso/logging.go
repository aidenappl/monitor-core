package sso

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/aidenappl/monitor-core/telemetry"
)

// backchannelCode matches the stable code go-forta/sso puts at the front of
// every back-channel logout line: "WARN SSO_BACKCHANNEL_PROVIDER_FAILED: …".
var backchannelCode = regexp.MustCompile(`^(?:WARN )?SSO_BACKCHANNEL_([A-Z_]+):`)

// libraryLogf routes go-forta/sso's printf hook into telemetry, for the
// back-channel path, which has no context to give.
func libraryLogf(format string, args ...any) {
	libraryLogfCtx(context.Background(), format, args...)
}

// libraryLogfCtx is the same, for go-forta's LogfCtx hook: the ctx is the one
// the Check ran under, so the event carries the request id that caused the
// checkpoint without the id being appended to the text.
//
// The library logs free text with a severity prefix and, on the back-channel
// path, a stable code. Both become event NAMES, so the lines group — the
// formatted line rides along as the message. Warnings are coalesced: the
// checkpoint runs on every session-authenticated request, and an IdP outage
// fails every one of them the same way.
func libraryLogfCtx(ctx context.Context, format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	data := map[string]any{"message": message}

	if m := backchannelCode.FindStringSubmatch(message); m != nil {
		name := "sso.backchannel." + strings.ToLower(m[1])
		if strings.HasPrefix(message, "WARN ") {
			telemetry.WarnCoalesced(ctx, name, name, nil, data)
		} else {
			telemetry.Info(ctx, name, data)
		}
		return
	}

	switch {
	case strings.Contains(message, "ending session"):
		telemetry.Info(ctx, "sso.session.revoked", data)
	case strings.Contains(message, "failed"):
		telemetry.WarnCoalesced(ctx, "sso.checkpoint.failed", "sso.checkpoint.failed", nil, data)
	case strings.Contains(message, "unavailable"):
		telemetry.WarnCoalesced(ctx, "sso.checkpoint.unavailable", "sso.checkpoint.unavailable", nil, data)
	default:
		telemetry.Debug(ctx, "sso.checkpoint.trace", data)
	}
}
