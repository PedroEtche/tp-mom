package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	connection *amqp.Connection
	name       string
	keys       []string
	channel    *amqp.Channel
}

func NewExchange(name string, keys []string, connectionSettings m.ConnSettings) (*ExchangeMiddleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://guest:guest@%s:%v/", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		// NOTE: No enmascaro el error del Dial, para no poder precision sobre el mismo (e.g. URI mal formado)
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		conn.Close()
		return nil, m.ErrMessageMiddlewareMessage
	}

	err = ch.ExchangeDeclare(
		name,     // name
		"direct", // type
		true,     // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		// NOTE: Si no se pudo declarar el exchange, cierro la conexion
		conn.Close()
		return nil, m.ErrMessageMiddlewareMessage
	}

	return &ExchangeMiddleware{connection: conn, name: name, keys: keys, channel: ch}, nil
}

func (em *ExchangeMiddleware) queueDeclareSetUp() (amqp.Queue, error) {
	q, err := em.channel.QueueDeclare(
		"",    // name
		true,  // durability
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return amqp.Queue{}, m.ErrMessageMiddlewareDisconnected
		}
		return amqp.Queue{}, m.ErrMessageMiddlewareMessage
	}
	return q, nil
}

func (em *ExchangeMiddleware) channelConsumeSetUp(q amqp.Queue) (<-chan amqp.Delivery, error) {
	// NOTE: El tag del consumer coincide con el nombre del exchange. No deberia surgir problema con esto ya que se abre un
	// channel nuevo para cada ExchangeMiddleware
	msgs, err := em.channel.Consume(
		q.Name,  // queue
		em.name, // consumer
		false,   // auto ack
		false,   // exclusive
		false,   // no local
		false,   // no wait
		nil,     // args
	)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		return nil, m.ErrMessageMiddlewareMessage
	}
	return msgs, nil
}

func (em *ExchangeMiddleware) bindQueueToKeys(q amqp.Queue) error {
	for _, key := range em.keys {
		err := em.channel.QueueBind(
			q.Name,  // queue name
			key,     // routing key
			em.name, // exchange
			false,
			nil)
		if err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// MIDDLEWARE INTERFACE IMPLEMENTATION
// -----------------------------------------------------------------------------

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	q, err := em.queueDeclareSetUp()
	if err != nil {
		return err
	}

	if err = em.bindQueueToKeys(q); err != nil {
		return err
	}

	msgs, err := em.channelConsumeSetUp(q)
	if err != nil {
		return err
	}

	for d := range msgs {
		copy := d
		callbackFunc(m.Message{Body: string(copy.Body)}, func() { copy.Ack(false) }, func() { copy.Nack(false, false) })
	}

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	// NOTE: En este caso em.name hace alusion al nombre del consumer
	if err := em.channel.Cancel(em.name, false); err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (em *ExchangeMiddleware) Send(msg m.Message) (err error) {
	for _, key := range em.keys {
		err = em.channel.Publish(
			em.name, // exchange
			key,     // routing key
			false,   // mandatory
			false,   // immediate
			amqp.Publishing{
				ContentType:  "text/plain",
				Body:         []byte(msg.Body),
				DeliveryMode: amqp.Persistent, // NOTE: Aseguro que el mensaje sea persistente para que no se pierda en caso de que el broker se caiga
			})
		if err != nil {
			if errors.Is(err, amqp.ErrClosed) {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}

	}

	return nil
}

func (em *ExchangeMiddleware) Close() error {
	if err := em.channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := em.connection.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
