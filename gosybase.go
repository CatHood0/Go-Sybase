package gosybase

import (
	"errors"
	"fmt"
	"log"

	sybase "github.com/CatHood0/Go-Sybase/internal"
)

type Database struct {
	db        *sybase.Sybase
	Connected bool
}

func Connect(propertiesPath string, log bool, customTdsLink string) (*Database, error) {
	sybaseDatabase, err := sybase.NewConnectionInstance(sybase.Config{
		Logs:          log,
		TdsLink:       customTdsLink,
		TdsProperties: propertiesPath,
	})

	if err != nil {
		return nil, err
	}

	connErr := sybaseDatabase.Connect()

	if connErr != nil {
		sybaseDatabase = nil
		return nil, connErr
	}

	return &Database{
		db:        sybaseDatabase,
		Connected: true,
	}, nil
}

func ConnectWithConfigs(serverConfig sybase.Config) (*Database, error) {
	sybaseDatabase, err := sybase.NewConnectionInstance(serverConfig)

	if err != nil {
		return nil, err
	}

	connErr := sybaseDatabase.Connect()

	if connErr != nil {
		sybaseDatabase = nil
		return nil, connErr
	}

	return &Database{
		db: sybaseDatabase,
	}, nil
}

func (ds *Database) RawQuery(query string) (*sybase.RawResponse, error) {
	if !ds.Connected {
		return nil, errors.New("Database isn't connected")
	}

	response, err := ds.db.Raw(query)

	if err != nil {
		log.Default().Print(err)
		return nil, fmt.Errorf("unable to execute the query by: %s", err)
	}

	return response, nil
}

func (ds *Database) QueryFirst(query string) (map[string]any, error) {
	data := map[string]any{}

	response, err := ds.db.Raw(query)

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
	if !ds.Connected {
		return errors.New("Database isn't connected")
	}
	response, err := ds.db.Raw(query)

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
	if !ds.Connected {
		return nil, errors.New("Database isn't connected")
	}
	value, err := ds.db.Raw(query)

	if err != nil {
		log.Default().Print(err)
		return nil, fmt.Errorf("unable to execute the query by: %s", err)
	}

	return value, nil
}

func (ds *Database) Disconnect() error {
	err := ds.db.Disconnect()
	ds.Connected = false
	return err
}
