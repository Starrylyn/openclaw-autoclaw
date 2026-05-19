export const PROTOCOL_VERSION = 4 as const;
export const MIN_CLIENT_PROTOCOL_VERSION = 4 as const;
// Restart/update health probes must stay compatible with the previous CLI
// protocol so an older updater can verify a newly restarted gateway.
export const MIN_PROBE_PROTOCOL_VERSION = 3 as const;
