package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queue      amqp.Queue
}

func NewQueue(name string, connectionSettings m.ConnSettings) (*QueueMiddleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		// NOTE: No enmascaro el error del Dial, para no poder precision sobre el mismo (e.g. URI mal formado)
		return nil, err
	}

	// NOTE: Creo un nuevo channel para cada nuevo QueueMiddleware. Segun los docs de RabbitMQ (https://www.rabbitmq.com/docs/channels), internamente van a utilizar la misma conexion TCP para evitar overhead
	ch, err := conn.Channel()
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		// NOTE: Si no se pudo declarar el channel, cierro la conexion
		conn.Close()
		return nil, m.ErrMessageMiddlewareMessage
	}

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
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		// NOTE: Si no se pudo declarar la queue, cierro la conexion (amqp asegura que cierra el channel frente a un error)
		conn.Close()
		return nil, m.ErrMessageMiddlewareMessage
	}

	return &QueueMiddleware{connection: conn, channel: ch, queue: q}, nil
}

func (qm *QueueMiddleware) channelConsumeSetUp() (<-chan amqp.Delivery, error) {
	// NOTE: Que el tag del consumer sea el mismo que el de la queue no deberia traer problema porque cada QueueMiddleware
	// crea un nuevo channel (https://www.rabbitmq.com/docs/channels)
	msgs, err := qm.channel.Consume(
		qm.queue.Name, // queue
		qm.queue.Name, // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		return nil, m.ErrMessageMiddlewareMessage
	}
	return msgs, nil
}

// -----------------------------------------------------------------------------
// MIDDLEWARE INTERFACE IMPLEMENTATION
// -----------------------------------------------------------------------------

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	msgs, err := qm.channelConsumeSetUp()
	if err != nil {
		return err
	}

	for d := range msgs {
		copy := d
		callbackFunc(m.Message{Body: string(copy.Body)}, func() { copy.Ack(false) }, func() { copy.Nack(false, false) })
	}

	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	// NOTE: En este caso qm.queue.Name hace alusion al nombre del consumer
	if err := qm.channel.Cancel(qm.queue.Name, false); err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
	err = qm.channel.Publish(
		"",            // exchange
		qm.queue.Name, // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
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
