package factory

import (
	"fmt"
	"sync"
	"sync/atomic"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	uri        string
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
	name       string
	stateMutex sync.Mutex
	stopSignal atomic.Bool
}

// tryDial intenta establecer una conexión con RabbitMQ utilizando la URI proporcionada.
// Si la conexión falla, reintenta con un backoff exponencial hasta alcanzar el número máximo de reintentos.
func tryDial(uri string, retries int) (*amqp.Connection, error) {
	return retryWithBackoff(
		retries,
		func() (*amqp.Connection, error) {
			return amqp.Dial(uri)
		},
		nil,
	)
}

func tryCreateChannel(conn *amqp.Connection, uri string, retries int) (*amqp.Connection, *amqp.Channel, error) {
	newConn := conn

	ch, err := retryWithBackoff(
		retries,
		func() (*amqp.Channel, error) {
			return newConn.Channel()
		},
		func() error {
			var dialErr error
			previousConn := newConn
			newConn, dialErr = tryDial(uri, retries)
			if dialErr == nil {
				closeIfReplaced(previousConn, newConn)
			}
			return dialErr
		},
	)
	if err != nil {
		return nil, nil, mapAMQPError(err)
	}

	return newConn, ch, nil
}

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
		return amqp.Queue{}, nil, nil, mapAMQPError(err)
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

	closeResourcesIfReplaced(previousChannel, ch, previousConn, c)

	qm.queue = q
	qm.channel = ch
	qm.connection = c

	return nil
}

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

	return &QueueMiddleware{uri: uri, connection: conn, channel: ch, queue: q, name: name}, nil
}

// -----------------------------------------------------------------------------
// MIDDLEWARE INTERFACE IMPLEMENTATION
// -----------------------------------------------------------------------------

func (qm *QueueMiddleware) tryConsuming(retries int) (msgs <-chan amqp.Delivery, err error) {
	msgCh, err := retryWithBackoff(
		retries,
		func() (<-chan amqp.Delivery, error) {
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
		},
		func() error {
			return qm.recoverQueueResources(defaultRetries)
		},
	)
	if err != nil {
		return msgCh, mapAMQPError(err)
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

func (qm *QueueMiddleware) tryStopConsuming(retries int) (err error) {
	_, err = retryWithBackoff(
		retries,
		func() (struct{}, error) {
			qm.stateMutex.Lock()
			defer qm.stateMutex.Unlock()

			placeHolder := struct{}{}
			// NOTE: En este caso qm.queue.Name hace alusion al nombre del consumer
			result := qm.channel.Cancel(qm.queue.Name, false)
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

func (qm *QueueMiddleware) tryPublish(msg m.Message, retries int) (err error) {
	_, err = retryWithBackoff(
		retries,
		func() (struct{}, error) {
			qm.stateMutex.Lock()
			defer qm.stateMutex.Unlock()

			placeHolder := struct{}{}
			result := qm.channel.Publish(
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
			return placeHolder, result
		},
		func() error {
			return qm.recoverQueueResources(defaultRetries)
		},
	)
	if err != nil {
		return mapAMQPError(err)
	}

	return nil
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
	return qm.tryPublish(msg, defaultRetries)
}

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
