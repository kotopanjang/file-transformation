package redis

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
)

func TestNewRedis(t *testing.T) {
	redisServer, _ := miniredis.Run()
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer.Addr(),
	})

	type args struct {
		addr     string
		password string
	}
	tests := []struct {
		name string
		args args
		want *RedisStorage
	}{
		{
			name: "newredis_ok",
			args: args{
				addr:     "1278.0.0.1:7777",
				password: "",
			},
			want: &RedisStorage{
				client: redisClient,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewRedis(tt.args.addr, tt.args.password); got == nil {
				t.Errorf("NewRedis() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRedisStorage_Set(t *testing.T) {
	redisServer, _ := miniredis.Run()
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer.Addr(),
	})

	key := "test-1"
	dataset := "string"
	type fields struct {
		client *redis.Client
	}
	type args struct {
		key     string
		dataset any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "set_success",
			fields: fields{
				client: redisClient,
			},
			args: args{
				key:     key,
				dataset: dataset,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisStorage{
				client: tt.fields.client,
			}
			if err := r.Set(tt.args.key, tt.args.dataset); (err != nil) != tt.wantErr {
				t.Errorf("RedisStorage.Set() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRedisStorage_GetByKey(t *testing.T) {
	redisServer, _ := miniredis.Run()
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisServer.Addr(),
	})

	type sample struct {
		Name string
	}
	key := "test-1"
	dataset := `{"Name": "rahadian"}`
	targetdataset := &sample{}

	type fields struct {
		client *redis.Client
	}
	type args struct {
		key          string
		dataset      string
		convertValue any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "getByKey_found",
			fields: fields{
				client: redisClient,
			},
			args: args{
				key:          key,
				dataset:      dataset,
				convertValue: targetdataset,
			},
			wantErr: false,
		},
		{
			name: "getByKey_notfound",
			fields: fields{
				client: redisClient,
			},
			args: args{
				key:          key,
				convertValue: "string",
			},
			wantErr: false,
		},
		{
			name: "getByKey_found_err1",
			fields: fields{
				client: redisClient,
			},
			args: args{
				key:          key,
				dataset:      dataset,
				convertValue: "targetdataset",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RedisStorage{
				client: tt.fields.client,
			}

			if tt.name == "getByKey_found" || tt.name == "getByKey_found_err1" {
				if err := r.Set(tt.args.key, tt.args.dataset); (err != nil) != tt.wantErr {
					t.Errorf("RedisStorage.Set() error = %v, wantErr %v", err, tt.wantErr)
				}
			}

			if err := r.GetByKey(tt.args.key, tt.args.convertValue); (err != nil) != tt.wantErr {
				t.Errorf("RedisStorage.GetByKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
