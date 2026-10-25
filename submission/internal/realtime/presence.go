package realtime

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// All timestamps and lease decisions use Redis server time in one atomic script.
const leaseLua = `
local t=redis.call('TIME')
local now=t[1]*1000+math.floor(t[2]/1000)
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
if ARGV[1]=='touch' then
 redis.call('ZADD',KEYS[1],now+tonumber(ARGV[3]),ARGV[2])
 redis.call('PEXPIRE',KEYS[1],tonumber(ARGV[3])*2)
 redis.call('SET',KEYS[2],now)
elseif ARGV[1]=='remove' then
 redis.call('ZREM',KEYS[1],ARGV[2])
end
return {redis.call('ZCARD',KEYS[1]),redis.call('GET',KEYS[2]) or ''}
`

var leaseScript = redis.NewScript(leaseLua)

type Presence struct {
	UserID   string     `json:"user_id"`
	Online   bool       `json:"online"`
	LastSeen *time.Time `json:"last_seen"`
}
type Leases struct {
	Redis  *redis.Client
	Prefix string
	TTL    time.Duration
}

func (l Leases) apply(ctx context.Context, user, connection, op string) (Presence, error) {
	p := Presence{UserID: user}
	v, err := leaseScript.Run(ctx, l.Redis, []string{l.Prefix + "leases:" + user, l.Prefix + "seen:" + user}, op, connection, l.TTL.Milliseconds()).Slice()
	if err != nil {
		return p, err
	}
	p.Online = v[0].(int64) > 0
	if raw := v[1].(string); raw != "" {
		ms, e := strconv.ParseInt(raw, 10, 64)
		if e != nil {
			return p, e
		}
		t := time.UnixMilli(ms).UTC()
		p.LastSeen = &t
	}
	return p, nil
}
func (l Leases) Touch(ctx context.Context, user, connection string) error {
	_, err := l.apply(ctx, user, connection, "touch")
	return err
}

type Renewal struct {
	User, Connection string
}

// TouchBatch preserves one atomic lease operation per connection, while sharing
// the network round trip. Individual command errors remain individual failures.
func (l Leases) TouchBatch(ctx context.Context, renewals []Renewal) []error {
	commands := make([]*redis.Cmd, len(renewals))
	_, batchErr := l.Redis.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, renewal := range renewals {
			// EVAL avoids a NOSCRIPT failure after Redis restart or script eviction.
			commands[i] = pipe.Eval(ctx, leaseLua, []string{l.Prefix + "leases:" + renewal.User, l.Prefix + "seen:" + renewal.User}, "touch", renewal.Connection, l.TTL.Milliseconds())
		}
		return nil
	})
	errors := make([]error, len(commands))
	for i, command := range commands {
		errors[i] = command.Err()
		if errors[i] == nil && command.Val() == nil {
			errors[i] = batchErr
		}
	}
	return errors
}
func (l Leases) Remove(ctx context.Context, user, connection string) error {
	_, err := l.apply(ctx, user, connection, "remove")
	return err
}
func (l Leases) Get(ctx context.Context, user string) (Presence, error) {
	return l.apply(ctx, user, "", "get")
}
