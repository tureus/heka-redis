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
	BatchSize   int    `toml:"batchsize"`
}

type RedisListInput struct {
	conf *RedisListInputConfig
	conn redis.Conn
	batchlpop *redis.Script
}

func (rli *RedisListInput) ConfigStruct() interface{} {
	return &RedisListInputConfig{":6379", "logs", "", 1}
}

func (rli *RedisListInput) Init(config interface{}) error {
	rli.conf = config.(*RedisListInputConfig)

	var err error
	rli.conn, err = redis.Dial("tcp", rli.conf.Address)
	if err != nil {
		return fmt.Errorf("connecting to - %s", err.Error())
	}

  rli.batchlpop = redis.NewScript(1, `
	  local i = tonumber(ARGV[1])
	  local res = {}

	  local length = redis.call('llen',KEYS[1])
	  if length < i then i = length end
	  while (i > 0) do
	    local item = redis.call("lpop", KEYS[1])
	    if (not item) then
	      break
	    end
	    table.insert(res, item)
	    i = i-1
	  end
	  return res
	`)

	return nil
}

func (rli *RedisListInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) error {
	var (
		dRunner pipeline.DecoderRunner
		decoder pipeline.Decoder
		ok      bool
		e       error
		reply   interface{}
		vals    []string
		msg     string
	)

	if rli.conf.DecoderName != "" {
		if dRunner, ok = h.DecoderRunner(rli.conf.DecoderName, fmt.Sprintf("%s-%s", ir.Name(), rli.conf.DecoderName)); !ok {
			return fmt.Errorf("Decoder not found: %s", rli.conf.DecoderName)
		}
		decoder = dRunner.Decoder()
	}

	for {
		reply, e = rli.conn.Do("BLPOP", rli.conf.ListName, "0")
		if e == nil {
			vals, e = redis.Strings(reply, nil)
			msg = vals[1]

			if e == nil {
				rli.InsertMessage(ir, decoder, msg)
			}
		}

		reply, e = rli.batchlpop.Do(rli.conn, rli.conf.ListName, rli.conf.BatchSize)

		if e == nil {
			vals, e = redis.Strings(reply, nil)

			if e == nil {
				for _,msg = range vals {
					rli.InsertMessage(ir, decoder, msg)
				}
			} else {
				fmt.Printf("err: %v\n", e)
			}
		} else {
			fmt.Printf("type: %T, error: %v\n", reply, e)
			return e
		}
	}

	return nil
}

func (rli *RedisListInput) InsertMessage(ir pipeline.InputRunner, decoder pipeline.Decoder, msg string) {
	var (
		pack    *pipeline.PipelinePack
		e error
	)
	// Get the InputRunner's chan to receive empty PipelinePacks
	packSupply := ir.InChan()

	pack = <-packSupply
	pack.Message.SetType("redis_list")
	// pack.Message.SetLogger(n.Channel)
	pack.Message.SetPayload(msg)
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
			ir.LogError(fmt.Errorf("Couldn't parse %s", msg))
		}
		pack.Recycle()
	}
}

func (rli *RedisListInput) Stop() {
	rli.conn.Close()
}

func init() {
	pipeline.RegisterPlugin("RedisListInput", func() interface{} {
		return new(RedisListInput)
	})
}
