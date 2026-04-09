package factory

import (
	"fmt"
	"sync"
	"sync/atomic"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	uri        string
	connection *amqp.Connection
	name       string
	keys       []string
	channel    *amqp.Channel
	stateMutex sync.Mutex
	stopSignal atomic.Bool
}

// -----------------------------------------------------------------------------
// CONSTRUCTOR
// -----------------------------------------------------------------------------

func NewExchange(name string, keys []string, connectionSettings m.ConnSettings) (*ExchangeMiddleware, error) {
	uri := fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := tryDial(uri, defaultRetries)
	if err != nil {
		return nil, err
	}

	conn, ch, err := tryCreateChannel(conn, uri, defaultRetries)
	if err != nil {
		conn.Close()
		return nil, err
	}

	ch, conn, err = tryDeclareExchange(ch, name, conn, uri, defaultRetries)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &ExchangeMiddleware{uri: uri, connection: conn, name: name, keys: keys, channel: ch}, nil
}

// -----------------------------------------------------------------------------
// RESOURCE RECOVERY
// -----------------------------------------------------------------------------

func tryDeclareExchange(ch *amqp.Channel, name string, conn *amqp.Connection, uri string, retries int) (*amqp.Channel, *amqp.Connection, error) {
	c := conn

	_, err := retryWithBackoff(
		retries,
		func() (struct{}, error) {
			placeHolder := struct{}{}
			err := ch.ExchangeDeclare(
				name,     // name
				"direct", // type
				true,     // durability
				false,    // auto-deleted
				false,    // internal
				false,    // no-wait
				nil,      // arguments
			)
			return placeHolder, err
		},
		func() error {
			var recoverErr error
			previousConn := c
			previousChannel := ch
			c, ch, recoverErr = tryCreateChannel(c, uri, retries)
			if recoverErr == nil {
				closeResourcesIfReplaced(previousChannel, ch, previousConn, c)
			}
			return recoverErr
		},
	)
	if err != nil {
		return nil, nil, normalizeMiddlewareErr(err)
	}

	return ch, c, nil
}

func (em *ExchangeMiddleware) recoverExchangeResources(retries int) error {
	em.stateMutex.Lock()
	defer em.stateMutex.Unlock()

	previousConn := em.connection
	previousChannel := em.channel

	ch, c, err := tryDeclareExchange(em.channel, em.name, em.connection, em.uri, retries)
	if err != nil {
		return err
	}

	closeResourcesIfReplaced(previousChannel, ch, previousConn, c)

	em.channel = ch
	em.connection = c

	return nil
}

func (em *ExchangeMiddleware) recoverAndRecreateQueue() (amqp.Queue, error) {
	if err := em.recoverExchangeResources(defaultRetries); err != nil {
		return amqp.Queue{}, err
	}

	return em.queueDeclareSetUp()
}

// -----------------------------------------------------------------------------
// CONSUME FLOW
// -----------------------------------------------------------------------------

func (em *ExchangeMiddleware) setUpConsumerQueue() (amqp.Queue, error) {
	q, err := em.queueDeclareSetUp()
	if err != nil {
		return amqp.Queue{}, err
	}

	q, err = em.bindQueueToKeys(q)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}

func (em *ExchangeMiddleware) queueDeclareSetUp() (amqp.Queue, error) {
	q, err := retryWithBackoff(
		defaultRetries,
		func() (amqp.Queue, error) {
			em.stateMutex.Lock()
			defer em.stateMutex.Unlock()

			return em.channel.QueueDeclare(
				"",    // name
				true,  // durability
				true,  // delete when unused
				false, // exclusive
				false, // no-wait
				nil,   // arguments
			)
		},
		func() error {
			return em.recoverExchangeResources(defaultRetries)
		},
	)
	if err != nil {
		return amqp.Queue{}, normalizeMiddlewareErr(err)
	}
	return q, nil
}

func (em *ExchangeMiddleware) channelConsumeSetUp(q amqp.Queue) (<-chan amqp.Delivery, error) {
	// NOTE: El tag del consumer coincide con el nombre del exchange. No deberia surgir problema con esto ya que se abre un
	// channel nuevo para cada ExchangeMiddleware
	msgs, err := retryWithBackoff(
		defaultRetries,
		func() (<-chan amqp.Delivery, error) {
			em.stateMutex.Lock()
			defer em.stateMutex.Unlock()

			return em.channel.Consume(
				q.Name,  // queue
				em.name, // consumer
				false,   // auto ack
				false,   // exclusive
				false,   // no local
				false,   // no wait
				nil,     // args
			)
		},
		func() error {
			recreatedQueue, setupErr := em.setUpConsumerQueue()
			if setupErr != nil {
				return setupErr
			}

			q = recreatedQueue
			return nil
		},
	)
	if err != nil {
		return nil, normalizeMiddlewareErr(err)
	}
	return msgs, nil
}

func (em *ExchangeMiddleware) bindQueueToKeys(q amqp.Queue) (amqp.Queue, error) {
	_, err := retryWithBackoff(
		defaultRetries,
		func() (struct{}, error) {
			em.stateMutex.Lock()
			defer em.stateMutex.Unlock()

			placeHolder := struct{}{}
			for _, key := range em.keys {
				if bindErr := em.channel.QueueBind(
					q.Name,  // queue name
					key,     // routing key
					em.name, // exchange
					false,
					nil,
				); bindErr != nil {
					return placeHolder, bindErr
				}
			}

			return placeHolder, nil
		},
		func() error {
			recreatedQueue, setupErr := em.recoverAndRecreateQueue()
			if setupErr != nil {
				return setupErr
			}

			q = recreatedQueue
			return nil
		},
	)
	if err != nil {
		return amqp.Queue{}, normalizeMiddlewareErr(err)
	}

	return q, nil
}

func (em *ExchangeMiddleware) tryConsuming(retries int) (msgs <-chan amqp.Delivery, err error) {
	msgCh, err := retryWithBackoff(
		retries,
		func() (<-chan amqp.Delivery, error) {
			q, setupErr := em.setUpConsumerQueue()
			if setupErr != nil {
				return nil, setupErr
			}

			return em.channelConsumeSetUp(q)
		},
		func() error {
			return em.recoverExchangeResources(defaultRetries)
		},
	)
	if err != nil {
		return msgCh, normalizeMiddlewareErr(err)
	}

	return msgCh, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	em.stopSignal.Store(false)

	for {
		msgs, err := em.tryConsuming(defaultRetries)
		if err != nil {
			return err
		}

		for d := range msgs {
			copy := d
			callbackFunc(m.Message{Body: string(copy.Body)}, func() { copy.Ack(false) }, func() { copy.Nack(false, false) })
		}

		// Si se pidió un stop explícito no reintento consumir.
		if em.stopSignal.Load() {
			return nil
		}
	}
}

// -----------------------------------------------------------------------------
// STOP FLOW
// -----------------------------------------------------------------------------

func (em *ExchangeMiddleware) tryStopConsuming(retries int) (err error) {
	_, err = retryWithBackoff(
		retries,
		func() (struct{}, error) {
			em.stateMutex.Lock()
			defer em.stateMutex.Unlock()

			_placeHolder := struct{}{}
			// NOTE: En este caso em.name hace alusion al nombre del consumer
			result := em.channel.Cancel(em.name, false)
			return _placeHolder, result
		},
		func() error {
			return em.recoverExchangeResources(defaultRetries)
		},
	)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	em.stopSignal.Store(true)
	return em.tryStopConsuming(defaultRetries)
}

// -----------------------------------------------------------------------------
// PUBLISH FLOW
// -----------------------------------------------------------------------------

func (em *ExchangeMiddleware) tryPublish(key string, msg m.Message, retries int) (err error) {
	_, err = retryWithBackoff(
		retries,
		func() (struct{}, error) {
			em.stateMutex.Lock()
			defer em.stateMutex.Unlock()

			placeHolder := struct{}{}
			result := em.channel.Publish(
				em.name, // exchange
				key,     // routing key
				false,   // mandatory
				false,   // immediate
				amqp.Publishing{
					ContentType:  "text/plain",
					Body:         []byte(msg.Body),
					DeliveryMode: amqp.Persistent, // NOTE: Aseguro que el mensaje sea persistente para que no se pierda en caso de que el broker se caiga
				})
			return placeHolder, result
		},
		func() error {
			return em.recoverExchangeResources(defaultRetries)
		},
	)
	if err != nil {
		return normalizeMiddlewareErr(err)
	}

	return nil
}

func (em *ExchangeMiddleware) Send(msg m.Message) (err error) {
	for _, key := range em.keys {
		if err = em.tryPublish(key, msg, defaultRetries); err != nil {
			return err
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// CLOSE
// -----------------------------------------------------------------------------

func (em *ExchangeMiddleware) Close() error {
	em.stopSignal.Store(true)
	if err := em.channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := em.connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
