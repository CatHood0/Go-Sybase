package gosybase

import (
	"errors"
	"fmt"
	"log"
)

type Database struct {
	Db        *sybase
	connected bool
}

func Connect(propertiesPath string, log bool, customTdsLink string) (*Database, error) {
	sybaseDatabase, err := newConnectionInstance(Config{
		Logs:          log,
		TdsLink:       customTdsLink,
		tdsProperties: propertiesPath,
	})

	if err != nil {
		return nil, err
	}

	connErr := sybaseDatabase.connect()

	if connErr != nil {
		sybaseDatabase = nil
		return nil, connErr
	}

	return &Database{
		Db:        sybaseDatabase,
		connected: true,
	}, nil
}

func ConnectWithConfigs(serverConfig Config) (*Database, error) {
	sybaseDatabase, err := newConnectionInstance(serverConfig)

	if err != nil {
		return nil, err
	}

	connErr := sybaseDatabase.connect()

	if connErr != nil {
		sybaseDatabase = nil
		return nil, connErr
	}

	return &Database{
		Db: sybaseDatabase,
	}, nil
}

func (ds *Database) RawQuery(query string) (*RawResponse, error) {
	if !ds.connected {
		return nil, errors.New("Database isn't connected")
	}

	response, err := ds.Db.raw(query)

	if err != nil {
		log.Default().Print(err)
		return nil, fmt.Errorf("unable to execute the query by: %s", err)
	}

	return response, nil
}

func (ds *Database) QueryFirst(query string) (map[string]any, error) {
	data := map[string]any{}

	response, err := ds.Db.raw(query)

	if err != nil {
		log.Default().Print(err)
		return data, fmt.Errorf("unable to execute the query by: %s", err)
	}

	if len(response.Results) < 1 {
		return data, fmt.Errorf("no result was found")
	}

	data = response.Results[0]

	return data, nil
}

func (ds *Database) Query(query string, callback func(map[string]any) error) error {
	if !ds.connected {
		return errors.New("Database isn't connected")
	}
	response, err := ds.Db.raw(query)

	if err != nil {
		log.Default().Print(err)
		return fmt.Errorf("unable to execute the query by: %s", err)
	}

	for _, result := range response.Results {
		callErr := callback(result)
		if callErr != nil {
			return callErr
		}
	}

	return nil
}

func (ds *Database) Exec(query string) (any, error) {
	if !ds.connected {
		return nil, errors.New("Database isn't connected")
	}
	value, err := ds.Db.raw(query)

	if err != nil {
		log.Default().Print(err)
		return nil, fmt.Errorf("unable to execute the query by: %s", err)
	}

	return value, nil
}

func (ds *Database) Disconnect() {
	ds.Db.disconnect()
	ds.connected = false
}
