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
		pop, err := rli.conn.Do("BLPOP", rli.conf.ListName)

		fmt.Printf("pop: %v, error: %v\n", err, pop)

		if err != nil {
			switch pop.(type) {
			case string:
				pack = <-packSupply
				pack.Message.SetType("redis_list")
				// pack.Message.SetLogger(n.Channel)
				pack.Message.SetPayload(pop.(string))
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
						ir.LogError(fmt.Errorf("Couldn't parse Redis message: %s", pop.(string)))
					}
					pack.Recycle()
				}
			}
		} else {
			fmt.Printf("error: %v\n", err)
			return err
		}

		// switch n := psc.Receive().(type) {
		// case redis.PMessage:
		// 	// Grab an empty PipelinePack from the InputRunner
		// 	pack = <-packSupply
		// 	pack.Message.SetType("redis_pub_sub")
		// 	pack.Message.SetLogger(n.Channel)
		// 	pack.Message.SetPayload(string(n.Data))
		// 	pack.Message.SetTimestamp(time.Now().UnixNano())
		// 	var packs []*pipeline.PipelinePack
		// 	if decoder == nil {
		// 		packs = []*pipeline.PipelinePack{pack}
		// 	} else {
		// 		packs, e = decoder.Decode(pack)
		// 	}
		// 	if packs != nil {
		// 		for _, p := range packs {
		// 			ir.Inject(p)
		// 		}
		// 	} else {
		// 		if e != nil {
		// 			ir.LogError(fmt.Errorf("Couldn't parse Redis message: %s", n.Data))
		// 		}
		// 		pack.Recycle()
		// 	}
		// case redis.Subscription:
		// 	ir.LogMessage(fmt.Sprintf("Subscription: %s %s %d\n", n.Kind, n.Channel, n.Count))
		// 	if n.Count == 0 {
		// 		return errors.New("No channel to subscribe")
		// 	}
		// case error:
		// 	fmt.Printf("error: %v\n", n)
		// 	return n
		// }
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
