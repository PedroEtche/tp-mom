package factory

import (
	"errors"
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	uri        string
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
	name       string
}

func exponentialBackoffSleep(attempt int) {
	sleepDuration := time.Duration(2<<attempt) * time.Second // Exponential backoff: 2s, 4s, 8s
	time.Sleep(sleepDuration)
}

// tryDial intenta establecer una conexión con RabbitMQ utilizando la URI proporcionada.
// Si la conexión falla, reintenta con un backoff exponencial hasta alcanzar el número máximo de reintentos.
func tryDial(uri string, retries int) (*amqp.Connection, error) {
	var err error
	for i := range retries {
		conn, err := amqp.Dial(uri)
		if err == nil {
			return conn, nil
		}
		exponentialBackoffSleep(i)
	}

	return nil, err
}

func tryCreateChannel(conn *amqp.Connection, uri string, retries int) (*amqp.Connection, *amqp.Channel, error) {
	var err error
	newConn := conn

	for i := range retries {
		ch, err := newConn.Channel()
		if err == nil {
			return newConn, ch, nil
		}
		newConn, err = tryDial(uri, retries)
		if err != nil {
			break
		}
		exponentialBackoffSleep(i)
	}

	switch {
	case errors.Is(err, amqp.ErrClosed):
		return nil, nil, m.ErrMessageMiddlewareDisconnected
	default:
		return nil, nil, m.ErrMessageMiddlewareMessage
	}
}

func tryDeclareQueue(ch *amqp.Channel, name string, conn *amqp.Connection, uri string, retries int) (amqp.Queue, *amqp.Channel, *amqp.Connection, error) {
	var err error
	c := conn

	for i := range retries {
		q, err := ch.QueueDeclare(
			name,  // name
			true,  // durability
			false, // delete when unused
			false, // exclusive
			false, // no-wait
			amqp.Table{
				amqp.QueueTypeArg: amqp.QueueTypeQuorum,
			},
		)
		if err == nil {
			return q, ch, c, nil
		}
		c, ch, err = tryCreateChannel(conn, uri, retries)
		if err != nil {
			break
		}
		exponentialBackoffSleep(i)
	}

	switch {
	case errors.Is(err, amqp.ErrClosed):
		return amqp.Queue{}, nil, nil, m.ErrMessageMiddlewareDisconnected
	default:
		return amqp.Queue{}, nil, nil, m.ErrMessageMiddlewareMessage
	}
}

func NewQueue(name string, connectionSettings m.ConnSettings) (*QueueMiddleware, error) {
	uri := fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := tryDial(uri, 3)
	if err != nil {
		return nil, err
	}

	// NOTE: Creo un nuevo channel para cada nuevo QueueMiddleware. Segun los docs de RabbitMQ (https://www.rabbitmq.com/docs/channels),
	// internamente van a utilizar la misma conexion TCP para evitar overhead
	conn, ch, err := tryCreateChannel(conn, uri, 3)
	if err != nil {
		conn.Close()
		return nil, err
	}

	q, ch, conn, err := tryDeclareQueue(ch, name, conn, uri, 3)
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
	ch := qm.channel
	c := qm.connection
	var q amqp.Queue

	for i := range retries {
		// NOTE: Que el tag del consumer sea el mismo que el de la queue no deberia traer problema porque cada QueueMiddleware
		// crea un nuevo channel (https://www.rabbitmq.com/docs/channels)
		msgs, err = qm.channel.Consume(
			qm.queue.Name, // queue
			qm.queue.Name, // consumer
			false,         // auto-ack
			false,         // exclusive
			false,         // no-local
			false,         // no-wait
			nil,           // args
		)
		if err == nil {
			return msgs, nil
		}
		q, ch, c, err = tryDeclareQueue(ch, qm.name, qm.connection, qm.uri, 3)
		if err != nil {
			break
		}
		qm.queue = q
		qm.channel = ch
		qm.connection = c
		exponentialBackoffSleep(i)
	}

	switch {
	case errors.Is(err, amqp.ErrClosed):
		return msgs, m.ErrMessageMiddlewareDisconnected
	default:
		return msgs, m.ErrMessageMiddlewareMessage
	}
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	msgs, err := qm.tryConsuming(3)
	if err != nil {
		return err
	}

	for d := range msgs {
		copy := d
		callbackFunc(m.Message{Body: string(copy.Body)}, func() { copy.Ack(false) }, func() { copy.Nack(false, false) })
	}

	return nil
}

func (qm *QueueMiddleware) tryStopConsuming(retries int) (err error) {
	ch := qm.channel
	c := qm.connection
	var q amqp.Queue

	for i := range retries {
		// NOTE: En este caso qm.queue.Name hace alusion al nombre del consumer
		err = qm.channel.Cancel(qm.queue.Name, false)
		if err == nil {
			return nil
		}
		q, ch, c, err = tryDeclareQueue(ch, qm.name, qm.connection, qm.uri, 3)
		if err != nil {
			break
		}
		qm.queue = q
		qm.channel = ch
		qm.connection = c
		exponentialBackoffSleep(i)
	}

	return m.ErrMessageMiddlewareDisconnected
}

func (qm *QueueMiddleware) StopConsuming() error {
	return qm.tryStopConsuming(3)
}

func (qm *QueueMiddleware) tryPublish(msg m.Message, retries int) (err error) {
	ch := qm.channel
	c := qm.connection
	var q amqp.Queue

	for i := range retries {
		err = qm.channel.Publish(
			"",            // exchange
			qm.queue.Name, // routing key
			false,         // mandatory
			false,         // immediate
			amqp.Publishing{
				ContentType:  "text/plain",
				Body:         []byte(msg.Body),
				DeliveryMode: amqp.Persistent, // NOTE: Aseguro que el mensaje sea persistente para que no se pierda en caso de que el broker se caiga
			})
		if err == nil {
			return nil
		}
		q, ch, c, err = tryDeclareQueue(ch, qm.name, qm.connection, qm.uri, 3)
		if err != nil {
			break
		}
		qm.queue = q
		qm.channel = ch
		qm.connection = c
		exponentialBackoffSleep(i)
	}

	switch {
	case errors.Is(err, amqp.ErrClosed):
		return m.ErrMessageMiddlewareDisconnected
	default:
		return m.ErrMessageMiddlewareMessage
	}
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
	return qm.tryPublish(msg, 3)
}

func (qm *QueueMiddleware) Close() error {
	if err := qm.channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := qm.connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
