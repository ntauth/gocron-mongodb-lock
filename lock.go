package mongodb_lock

import (
	"context"
	"time"

	std_errors "errors"

	gocron "github.com/go-co-op/gocron/v2"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	DefaultKeyField = "key"
	DefaultTTLField = "expires_at"
	DefaultTTLValue = 5 * time.Minute
)

var (
	ErrLockIndexCouldNotCreate  = errors.New("could not create indices on either the unique or ttl field(s)")
	ErrParamIsNil               = errors.New("param(s) cannot be nil")
	ErrDuplicateKey             = errors.New("duplicate key error")
	ErrNotFoundKey              = errors.New("key does not exist")
	ErrCouldNotUnlock           = errors.New("could not unlock")
	ErrFailedToConnectToMongoDB = errors.New("failed to connect to mongodb")
)

type MongoDBLockerOptions struct {
	ExpireAfter  time.Duration
	LockTTL      time.Duration
	KeyField     string
	TTLField     string
	UnlockAlways bool
}

type MongoDBLockerOption func(*MongoDBLockerOptions)

func WithMongoDBLockerExpireAfter(expireAfter time.Duration) MongoDBLockerOption {
	return func(opts *MongoDBLockerOptions) {
		opts.ExpireAfter = expireAfter
	}
}

func WithMongoDBLockerKeyField(keyField string) MongoDBLockerOption {
	return func(opts *MongoDBLockerOptions) {
		opts.KeyField = keyField
	}
}

func WithMongoDBLockerTTLField(ttlField string) MongoDBLockerOption {
	return func(opts *MongoDBLockerOptions) {
		opts.TTLField = ttlField
	}
}

func WithMongoDBLockerLockTTL(lockTTL time.Duration) MongoDBLockerOption {
	return func(opts *MongoDBLockerOptions) {
		opts.LockTTL = lockTTL
	}
}

func WithMongoDBLockerUnlockAlways() MongoDBLockerOption {
	return func(opts *MongoDBLockerOptions) {
		opts.UnlockAlways = true
	}
}

type mongoDBLocker struct {
	opts MongoDBLockerOptions
	c    *mongo.Collection
}

var _ gocron.Locker = (*mongoDBLocker)(nil)

// NewMongoDBLockerAlways creates a new mongodb-backed distributed locker.
func NewMongoDBLocker(ctx context.Context, c *mongo.Collection, opts ...MongoDBLockerOption) (*mongoDBLocker, error) {
	if err := c.Database().Client().Ping(ctx, nil); err != nil {
		return nil, std_errors.Join(ErrFailedToConnectToMongoDB, err)
	}

	return newMongoDBLocker(ctx, c, opts...)
}

// NewMongoDBLockerAlways creates a new mongodb-backed distributed locker, even when pinging fails.
func NewMongoDBLockerAlways(ctx context.Context, c *mongo.Collection, opts ...MongoDBLockerOption) (*mongoDBLocker, error) {
	return newMongoDBLocker(ctx, c, opts...)
}

func newMongoDBLocker(ctx context.Context, c *mongo.Collection, opts ...MongoDBLockerOption) (*mongoDBLocker, error) {
	if c == nil {
		return nil, ErrParamIsNil
	}

	m := &mongoDBLocker{
		c: c,
		opts: MongoDBLockerOptions{
			KeyField: DefaultKeyField,
			TTLField: DefaultTTLField,
		},
	}
	for _, opt := range opts {
		opt(&m.opts)
	}

	if err := m.ensureMongoDBLockIndex(ctx, c, m.opts.ExpireAfter); err != nil {
		return nil, err
	}

	return m, nil
}

type mongoDBLock struct {
	key          string
	unlockAlways bool
	c            *mongo.Collection
}

var _ gocron.Lock = (*mongoDBLock)(nil)

func (ml *mongoDBLocker) Lock(ctx context.Context, key string, ttl *time.Duration) (gocron.Lock, error) {
	expiry := time.Now()

	if ttl != nil {
		expiry = expiry.Add(*ttl)
	} else if ml.opts.LockTTL != 0 {
		expiry = expiry.Add(ml.opts.LockTTL)
	} else {
		expiry = expiry.Add(DefaultTTLValue)
	}

	_, err := ml.c.InsertOne(ctx, bson.M{
		ml.opts.KeyField: key,
		"created_at":     time.Now(),
		ml.opts.TTLField: expiry,
	})

	if err != nil && mongo.IsDuplicateKeyError(err) {
		return nil, ErrDuplicateKey
	}

	return &mongoDBLock{
		key:          key,
		c:            ml.c,
		unlockAlways: ml.opts.UnlockAlways,
	}, nil
}

func (ml *mongoDBLock) Unlock(ctx context.Context) error {
	if ml.unlockAlways {
		ctx = context.Background()
	}
	res, err := ml.c.DeleteOne(ctx, bson.M{"key": ml.key})

	if err != nil {
		return ErrCouldNotUnlock
	} else if res.DeletedCount == 0 {
		return ErrNotFoundKey
	}

	return nil
}

func (ml *mongoDBLocker) ensureMongoDBLockIndex(ctx context.Context, c *mongo.Collection, expireAfter time.Duration) error {
	if c == nil {
		return ErrParamIsNil
	}

	if _, err := c.Indexes().CreateOne(
		ctx,
		mongo.IndexModel{
			Keys:    bson.D{{Key: ml.opts.KeyField, Value: 1}},
			Options: options.Index().SetUnique(true),
		},
	); err != nil {
		return errors.Wrap(err, ErrLockIndexCouldNotCreate.Error())
	}

	if _, err := c.Indexes().CreateOne(
		ctx,
		mongo.IndexModel{
			Keys: bson.D{{Key: ml.opts.TTLField, Value: 1}},
			// ExpireAfterSeconds mark documents to be deleted after X seconds from when the TTLField has become lower or equal to the current time.
			Options: options.Index().SetExpireAfterSeconds(int32(expireAfter.Seconds())),
		},
	); err != nil {
		return errors.Wrap(err, ErrLockIndexCouldNotCreate.Error())
	}

	return nil
}
