package factory

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	uri        string
	connection *amqp.Connection
	channel    *amqp.Channel
	publishAck <-chan amqp.Confirmation
	queue      amqp.Queue
	name       string
	stateMutex sync.Mutex
	stopSignal atomic.Bool
}

// -----------------------------------------------------------------------------
// CONSTRUCTOR
// -----------------------------------------------------------------------------

func NewQueue(name string, connectionSettings m.ConnSettings) (*QueueMiddleware, error) {
	uri := fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := tryDial(uri, defaultRetries)
	if err != nil {
		return nil, err
	}

	// NOTE: Creo un nuevo channel para cada nuevo QueueMiddleware. Segun los docs de RabbitMQ (https://www.rabbitmq.com/docs/channels),
	// internamente van a utilizar la misma conexion TCP para evitar overhead
	conn, ch, err := tryCreateChannel(conn, uri, defaultRetries)
	if err != nil {
		conn.Close()
		return nil, err
	}

	q, ch, conn, err := tryDeclareQueue(ch, name, conn, uri, defaultRetries)
	if err != nil {
		conn.Close()
		return nil, err
	}

	publishAck, err := configurePublisherConfirms(ch)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &QueueMiddleware{uri: uri, connection: conn, channel: ch, publishAck: publishAck, queue: q, name: name}, nil
}

func configurePublisherConfirms(ch *amqp.Channel) (<-chan amqp.Confirmation, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, normalizeMiddlewareErr(err)
	}

	return ch.NotifyPublish(make(chan amqp.Confirmation, 1)), nil
}

// -----------------------------------------------------------------------------
// RESOURCE RECOVERY
// -----------------------------------------------------------------------------

func tryDeclareQueue(ch *amqp.Channel, name string, conn *amqp.Connection, uri string, retries int) (amqp.Queue, *amqp.Channel, *amqp.Connection, error) {
	c := conn

	q, err := retryWithBackoff(
		retries,
		func() (amqp.Queue, error) {
			return ch.QueueDeclare(
				name,  // name
				true,  // durability
				false, // delete when unused
				false, // exclusive
				false, // no-wait
				amqp.Table{
					amqp.QueueTypeArg: amqp.QueueTypeQuorum,
				},
			)
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
		return amqp.Queue{}, nil, nil, normalizeMiddlewareErr(err)
	}

	return q, ch, c, nil
}

func (qm *QueueMiddleware) recoverQueueResources(retries int) error {
	qm.stateMutex.Lock()
	defer qm.stateMutex.Unlock()

	previousConn := qm.connection
	previousChannel := qm.channel

	q, ch, c, err := tryDeclareQueue(qm.channel, qm.name, qm.connection, qm.uri, retries)
	if err != nil {
		return err
	}

	publishAck, err := configurePublisherConfirms(ch)
	if err != nil {
		return err
	}

	closeResourcesIfReplaced(previousChannel, ch, previousConn, c)

	qm.queue = q
	qm.channel = ch
	qm.connection = c
	qm.publishAck = publishAck

	return nil
}

// -----------------------------------------------------------------------------
// CONSUME FLOW
// -----------------------------------------------------------------------------

func (qm *QueueMiddleware) consumeFromQueue() (<-chan amqp.Delivery, error) {
	qm.stateMutex.Lock()
	defer qm.stateMutex.Unlock()

	// NOTE: Que el tag del consumer sea el mismo que el de la queue no deberia traer problema porque cada QueueMiddleware
	// crea un nuevo channel (https://www.rabbitmq.com/docs/channels)
	return qm.channel.Consume(
		qm.queue.Name, // queue
		qm.queue.Name, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
}

func (qm *QueueMiddleware) tryConsuming(retries int) (msgs <-chan amqp.Delivery, err error) {
	msgCh, err := retryWithBackoff(
		retries,
		func() (<-chan amqp.Delivery, error) {
			return qm.consumeFromQueue()
		},
		func() error {
			return qm.recoverQueueResources(defaultRetries)
		},
	)
	if err != nil {
		return msgCh, normalizeMiddlewareErr(err)
	}

	return msgCh, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	qm.stopSignal.Store(false)

	for {
		msgs, err := qm.tryConsuming(defaultRetries)
		if err != nil {
			return err
		}

		for d := range msgs {
			copy := d
			callbackFunc(m.Message{Body: string(copy.Body)}, func() { copy.Ack(false) }, func() { copy.Nack(false, false) })
		}

		// Si se pidió un stop explícito no reintento consumir.
		if qm.stopSignal.Load() {
			return nil
		}
	}
}

// -----------------------------------------------------------------------------
// STOP FLOW
// -----------------------------------------------------------------------------

func (qm *QueueMiddleware) cancelConsuming() error {
	qm.stateMutex.Lock()
	defer qm.stateMutex.Unlock()

	// NOTE: En este caso qm.queue.Name hace alusion al nombre del consumer
	return qm.channel.Cancel(qm.queue.Name, false)
}

func (qm *QueueMiddleware) tryStopConsuming(retries int) (err error) {
	_, err = retryWithBackoff(
		retries,
		func() (struct{}, error) {
			placeHolder := struct{}{}
			result := qm.cancelConsuming()
			return placeHolder, result
		},
		func() error {
			return qm.recoverQueueResources(defaultRetries)
		},
	)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	qm.stopSignal.Store(true)
	return qm.tryStopConsuming(defaultRetries)
}

// -----------------------------------------------------------------------------
// PUBLISH FLOW
// -----------------------------------------------------------------------------

func (qm *QueueMiddleware) publishToQueue(msg m.Message) error {
	qm.stateMutex.Lock()
	defer qm.stateMutex.Unlock()

	err := qm.channel.Publish(
		"",            // exchange
		qm.queue.Name, // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
			DeliveryMode: amqp.Persistent, // NOTE: Aseguro que el mensaje sea persistente para que no se pierda en caso de que el broker se caiga
		},
	)
	if err != nil {
		return err
	}

	select {
	case confirmation, ok := <-qm.publishAck:
		if !ok {
			return m.ErrMessageMiddlewareDisconnected
		}

		if !confirmation.Ack {
			return m.ErrMessageMiddlewareMessage
		}
	case <-time.After(defaultPublishConfirmTimeout):
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (qm *QueueMiddleware) tryPublish(msg m.Message, retries int) (err error) {
	_, err = retryWithBackoff(
		retries,
		func() (struct{}, error) {
			placeHolder := struct{}{}
			result := qm.publishToQueue(msg)
			return placeHolder, result
		},
		func() error {
			return qm.recoverQueueResources(defaultRetries)
		},
	)
	if err != nil {
		return normalizeMiddlewareErr(err)
	}

	return nil
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
	return qm.tryPublish(msg, defaultRetries)
}

// -----------------------------------------------------------------------------
// CLOSE
// -----------------------------------------------------------------------------

func (qm *QueueMiddleware) Close() error {
	qm.stopSignal.Store(true)
	qm.stateMutex.Lock()
	defer qm.stateMutex.Unlock()

	if err := qm.channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := qm.connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
