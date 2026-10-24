CREATE TABLE users (
    id uuid PRIMARY KEY,
    display_name text NOT NULL CHECK (length(display_name) BETWEEN 1 AND 100)
);

CREATE TABLE conversations (
    id uuid PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('direct', 'group')),
    direct_key text UNIQUE,
    last_sequence bigint NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((kind = 'direct' AND direct_key IS NOT NULL) OR
           (kind = 'group' AND direct_key IS NULL))
);

CREATE TABLE members (
    conversation_id uuid NOT NULL REFERENCES conversations(id),
    user_id uuid NOT NULL REFERENCES users(id),
    read_sequence bigint NOT NULL DEFAULT 0 CHECK (read_sequence >= 0),
    PRIMARY KEY (conversation_id, user_id)
);
CREATE INDEX members_by_user ON members (user_id, conversation_id);

CREATE TABLE messages (
    conversation_id uuid NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    sender_id uuid NOT NULL,
    client_message_id uuid NOT NULL,
    body text NOT NULL CHECK (length(body) BETWEEN 1 AND 4000),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (conversation_id, sequence),
    UNIQUE (sender_id, client_message_id),
    FOREIGN KEY (conversation_id, sender_id)
        REFERENCES members (conversation_id, user_id)
);

CREATE TABLE outbox (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conversation_id uuid NOT NULL REFERENCES conversations(id),
    event_type text NOT NULL CHECK (event_type IN ('message', 'read')),
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_pending ON outbox (id) WHERE published_at IS NULL;
