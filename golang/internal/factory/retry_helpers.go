package factory

import (
	"errors"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const defaultRetries = 3

func exponentialBackoffSleep(attempt int) {
	sleepDuration := time.Duration(2<<attempt) * time.Second // Exponential backoff: 2s, 4s, 8s
	time.Sleep(sleepDuration)
}

func mapAMQPError(err error) error {
	if errors.Is(err, amqp.ErrClosed) {
		return m.ErrMessageMiddlewareDisconnected
	}
	return m.ErrMessageMiddlewareMessage
}

func normalizeMiddlewareErr(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, m.ErrMessageMiddlewareDisconnected) || errors.Is(err, m.ErrMessageMiddlewareMessage) {
		return err
	}

	return mapAMQPError(err)
}

func closeIfReplaced(current *amqp.Connection, replacement *amqp.Connection) {
	if current != nil && current != replacement {
		current.Close()
	}
}

func closeResourcesIfReplaced(currentCh *amqp.Channel, replacementCh *amqp.Channel, currentConn *amqp.Connection, replacementConn *amqp.Connection) {
	if currentCh != nil && currentCh != replacementCh {
		currentCh.Close()
	}

	closeIfReplaced(currentConn, replacementConn)
}

// retryWithBackoff Me permite hacer una operacion que puede llegar a fallar por perdida conexion,
// y tener una funcion de recovery en caso de fallar. Por ejemplo, si falla un Publish sobre un channel, debo
// volver a levantar el Channel y la topologia.
func retryWithBackoff[T any](retries int, operation func() (T, error), recoverFunc func() error) (T, error) {
	if retries <= 0 {
		// Pasar un valor de retries menor o igual a 0 va a caussar comportamiento erroneo,
		// Lanzo panic aqui para captar el error a penas suceda
		panic("retries must be greater than 0")
	}
	var defaultValue T
	var err error

	// En el ultimo intento fallido no tiene sentido recuperar ni dormir,
	// porque no habra una nueva operacion que aproveche esa recuperacion.
	for i := range retries + 1 {
		result, opErr := operation()
		if opErr == nil {
			return result, nil
		}

		err = opErr
		if i == retries-1 {
			break
		}

		if recoverFunc != nil {
			recoverErr := recoverFunc()
			if recoverErr != nil {
				err = recoverErr
				break
			}
		}

		exponentialBackoffSleep(i)
	}

	return defaultValue, err
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
		return nil, nil, normalizeMiddlewareErr(err)
	}

	return newConn, ch, nil
}
