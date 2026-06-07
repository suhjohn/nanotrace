CREATE TABLE IF NOT EXISTS nanotrace_oauth_states (
    token_hash text PRIMARY KEY,
    provider text NOT NULL,
    return_to text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS nanotrace_oauth_states_expires_at_idx
ON nanotrace_oauth_states (expires_at);

CREATE INDEX IF NOT EXISTS nanotrace_oauth_states_provider_idx
ON nanotrace_oauth_states (provider);
