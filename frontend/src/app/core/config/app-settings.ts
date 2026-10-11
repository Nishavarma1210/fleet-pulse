/**
 * Every timeout, retry cap, delay limit and threshold of the app lives here.
 * No other file should contain a "magic number" for these values.
 * The comments name the requirement from the assignment each value comes from.
 */
export const APP_SETTINGS = {
  api: {
    baseUrl: 'http://localhost:4000',
    /** Secondary read-only endpoint (stretch goal: circuit breaker). */
    secondaryBaseUrl: 'http://localhost:4001',
  },

  /** R3: HTTP resilience. */
  http: {
    /** R3.3: every request has a 10 s timeout. */
    timeoutMs: 10_000,
    /** R3.1: at most 3 attempts for a retryable GET. */
    maxAttempts: 3,
    /** R3.1: only these statuses are retried (network errors are retried too). */
    retryableStatuses: [429, 502, 503, 504],
    /** Wait before a retry when the server does not send Retry-After. */
    retryBaseDelayMs: 500,
    /** Upper limit for any retry wait, even when Retry-After asks for more. */
    retryMaxDelayMs: 5_000,
  },

  /** A: authentication. */
  auth: {
    /** Where the user lands after login when no earlier page was saved. */
    defaultRoute: '/fleet',
    /** Session storage key for the page to return to after a forced logout. */
    returnUrlKey: 'fleetpulse.returnUrl',
  },

  /** R1: transports, fallback and reconnect. */
  stream: {
    /** R1.2: WebSocket must connect within 5 s, otherwise fall back. */
    connectTimeoutMs: 5_000,
    /** R1.2: after 3 failures in a row the next transport is used. */
    maxConsecutiveFailures: 3,
    /** R1.1: polling interval of /metrics/snapshot. */
    pollIntervalMs: 2_000,
    /** R1.3: how often a higher transport is tried again. */
    climbBackIntervalMs: 30_000,
    /** R1.4: reconnect delay grows from base, doubling, up to the cap. */
    reconnectBaseDelayMs: 1_000,
    reconnectMaxDelayMs: 30_000,
    /** R1.4: random share of the delay removed, so clients do not reconnect in lockstep. */
    reconnectJitterRatio: 0.5,
    /** R1.5: the server pings every 5 s; two missed pings mean a dead connection. */
    pingIntervalMs: 5_000,
    missedPingsAllowed: 2,
    /** Extra time on top of the missed pings before the connection is declared dead. */
    heartbeatGraceMs: 1_000,
  },

  /** R2: data integrity. */
  integrity: {
    /** R2.2: events up to 1 s late are still put into the right order. */
    reorderWindowMs: 1_000,
    /** Largest number of events the reorder buffer may hold. */
    maxBufferedEvents: 20_000,
    /** Largest offset gap that is filled with /metrics/replay; bigger gaps reload the snapshot. */
    maxReplayGap: 50_000,
  },

  /** E: connection health. */
  health: {
    /** E.2: data older than 10 s is marked as stale. */
    staleAfterMs: 10_000,
    /** How often the "last event N s ago" text is refreshed. */
    statusTickMs: 1_000,
  },

  /** B and C: performance and display. */
  ui: {
    /** Performance target: under 60 table rows in the DOM at any time. */
    tableRowHeightPx: 44,
    tableMaxRenderedRows: 40,
    /** Incoming events are applied to the screen in one batch per interval. */
    eventBatchIntervalMs: 100,
    /** C.4: charts show at most 300 points per series. */
    chartMaxPoints: 300,
    /** C.1: the detail view shows the last 15 minutes. */
    chartWindowMinutes: 15,
    /** Search input is applied after this quiet time. */
    searchDebounceMs: 200,
    /** B.3: how long a changed status cell stays highlighted. */
    statusFlashMs: 1_200,
  },

  /** D: alert rules. */
  rules: {
    /** D.4: wait after the last keystroke before asking /rules/check-name. */
    nameCheckDebounceMs: 400,
    /** Metric limits used by the form validators (D.2). */
    percentRange: { min: 0, max: 100 },
    maxConditionsPerRule: 10,
    maxNameLength: 60,
  },
} as const;

export type AppSettings = typeof APP_SETTINGS;
