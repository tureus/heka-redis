package heka_redis

import (
	// "errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/mozilla-services/heka/pipeline"
	"time"
)

type RedisListInputConfig struct {
	Address     string `toml:"address"`
	ListName    string `toml:"listname"`
	DecoderName string `toml:"decoder"`
}

type RedisListInput struct {
	conf *RedisListInputConfig
	conn redis.Conn
}

func (rli *RedisListInput) ConfigStruct() interface{} {
	return &RedisListInputConfig{":6379", "logs", ""}
}

func (rli *RedisListInput) Init(config interface{}) error {
	rli.conf = config.(*RedisListInputConfig)

	var err error
	rli.conn, err = redis.Dial("tcp", rli.conf.Address)
	if err != nil {
		return fmt.Errorf("connecting to - %s", err.Error())
	}

	return nil
}

func (rli *RedisListInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) error {
	var (
		dRunner pipeline.DecoderRunner
		decoder pipeline.Decoder
		pack    *pipeline.PipelinePack
		e       error
		ok      bool
	)
	// Get the InputRunner's chan to receive empty PipelinePacks
	packSupply := ir.InChan()

	if rli.conf.DecoderName != "" {
		if dRunner, ok = h.DecoderRunner(rli.conf.DecoderName, fmt.Sprintf("%s-%s", ir.Name(), rli.conf.DecoderName)); !ok {
			return fmt.Errorf("Decoder not found: %s", rli.conf.DecoderName)
		}
		decoder = dRunner.Decoder()
	}

	for {
		reply, err := rli.conn.Do("BLPOP", rli.conf.ListName, "0")

		if err == nil {
			vals, extracterr := redis.Strings(reply, nil)

			if extracterr == nil {

				pack = <-packSupply
				pack.Message.SetType("redis_list")
				// pack.Message.SetLogger(n.Channel)
				pack.Message.SetPayload(vals[1])
				pack.Message.SetTimestamp(time.Now().UnixNano())

				var packs []*pipeline.PipelinePack
				if decoder == nil {
					packs = []*pipeline.PipelinePack{pack}
				} else {
					packs, e = decoder.Decode(pack)
				}

				if packs != nil {
					for _, p := range packs {
						ir.Inject(p)
					}
				} else {
					if e != nil {
						ir.LogError(fmt.Errorf("Couldn't parse Redis message: %s", vals))
					}
					pack.Recycle()
				}
			} else {
				fmt.Printf("err: %v\n", extracterr)
			}
		} else {
			fmt.Printf("type: %T, error: %v\n", reply, err)
			return err
		}
	}

	return nil
}

func (rli *RedisListInput) Stop() {
	rli.conn.Close()
}

func init() {
	pipeline.RegisterPlugin("RedisListInput", func() interface{} {
		return new(RedisListInput)
	})
}
