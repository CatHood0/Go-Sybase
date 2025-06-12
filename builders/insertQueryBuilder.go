package gosybasebuilder

import (
	"strings"
)

// InsertQuery representa una consulta de inserción SQL y sus componentes.
// Contiene:
//   - Conditions: Un slice de condiciones que forman partes de la consulta INSERT
//   - Schemas: Un mapa que define esquemas de base de datos para diferentes tablas
//     (permite especificar esquemas específicos o un esquema general para todas las tablas)
type InsertQuery struct {
	Conditions []Condition
	Schemas    map[string]string
}

// New crea y devuelve una nueva instancia de InsertQuery inicializada.
// Retorna:
//   - *InsertQuery: Puntero a una nueva estructura InsertQuery vacía
func NewInsert() *InsertQuery {
	return &InsertQuery{Conditions: []Condition{}, Schemas: map[string]string{}}
}

// DefineSchemas establece los esquemas de base de datos que se usarán para las tablas en la consulta.
// Parámetros:
//   - schemas: Mapa donde las claves son nombres de tabla y los valores son nombres de esquema
//
// Retorna:
//   - *InsertQuery: El mismo objeto InsertQuery para permitir encadenamiento de métodos
func (q *InsertQuery) DefineSchemas(schemas map[string]string) *InsertQuery {
	q.Schemas = schemas
	return q
}

// InsertTo especifica la tabla de destino para la inserción.
// Parámetros:
//   - to: Nombre de la tabla donde se insertarán los datos
//
// Retorna:
//   - *InsertQuery: El mismo objeto InsertQuery para permitir encadenamiento de métodos
func (q *InsertQuery) InsertTo(to string) *InsertQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "args", Query: getInsertSchema(to, q)})
	return q
}

// ToColumn especifica una sola columna de destino para la inserción.
// Parámetros:
//   - column: Nombre de la columna donde se insertarán los datos
//
// Retorna:
//   - *InsertQuery: El mismo objeto InsertQuery para permitir encadenamiento de métodos
func (q *InsertQuery) ToColumn(column string) *InsertQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "columns",
		Query:     " (" + *trim(column) + ")",
	})
	return q
}

// ToColumns especifica múltiples columnas de destino para la inserción.
// Parámetros:
//   - columns: Nombres de las columnas donde se insertarán los datos
//
// Retorna:
//   - *InsertQuery: El mismo objeto InsertQuery para permitir encadenamiento de métodos
func (q *InsertQuery) ToColumns(columns ...string) *InsertQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "columns",
		Query:     " (" + *trim(strings.Join(columns, ", ")) + ")",
	})
	return q
}

// Values especifica los valores a insertar para múltiples columnas.
// Parámetros:
//   - values: Valores a insertar (deben coincidir en número y orden con las columnas especificadas)
//
// Retorna:
//   - *InsertQuery: El mismo objeto InsertQuery para permitir encadenamiento de métodos
func (q *InsertQuery) Values(values ...string) *InsertQuery {
	last := q.Conditions[len(q.Conditions)-1]
	if last.TypeQuery == "continue_insertions" {
		q.Conditions = append(q.Conditions, Condition{
			TypeQuery: "continue_insertions",
			Query:     "(" + *trim(strings.Join(values, ", ")) + ")",
		})
		return q
	}
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "to_value",
		Query:     "(" + *trim(strings.Join(values, ", ")) + ")",
	})
	return q
}

// And permite agregar múltiples conjuntos de valores en una sola consulta INSERT.
// Retorna:
//   - *InsertQuery: El mismo objeto InsertQuery para permitir encadenamiento de métodos
func (q *InsertQuery) And() *InsertQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "continue_insertions",
		Query:     ", ",
	})
	return q
}

// Value especifica un solo valor a insertar (para una sola columna).
// Parámetros:
//   - value: Valor a insertar
//
// Retorna:
//   - *InsertQuery: El mismo objeto InsertQuery para permitir encadenamiento de métodos
func (q *InsertQuery) Value(value string) *InsertQuery {
	last := q.Conditions[len(q.Conditions)-1]

	if last.TypeQuery == "continue_insertions" {
		q.Conditions = append(q.Conditions, Condition{
			TypeQuery: "continue_insertions",
			Query:     "(" + *trim(value) + ")",
		})
		return q
	}
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "to_value",
		Query:     "(" + *trim(value) + ")",
	})
	return q
}

// BuildSQL construye y devuelve la cadena SQL completa para la consulta de inserción.
// Retorna:
//   - string: La consulta SQL completa terminada con punto y coma
func (q *InsertQuery) BuildSQL() string {
	conditions := q.Conditions
	if len(conditions) == 0 {
		return ""
	}
	query := "INSERT INTO "
	length := len(conditions)

	for i := range length {
		var end string = ""
		if i+1 >= length {
			end = ";"
		} else if q.Conditions[i].TypeQuery == "continue_insertions" {
			query += conditions[i].BuildQueryStr(false, true)
			continue
		}

		query += *trimRight(conditions[i].BuildQueryStr(false, true)) + end

	}
	return query
}

// getInsertSchema obtiene el esquema apropiado para una tabla basado en la configuración.
// Parámetros:
//   - from: Nombre de la tabla (puede incluir alias)
//   - q: Puntero a la estructura InsertQuery que contiene los esquemas configurados
//
// Retorna:
//   - string: Nombre de tabla con esquema (si está configurado) o solo nombre de tabla
func getInsertSchema(from string, q *InsertQuery) string {
	var schema string
	if len(q.Schemas) != 0 {
		table := strings.Split(from, " ")
		effectiveTableName := table[0]
		if q.Schemas[effectiveTableName] != "" {
			schema = q.Schemas[effectiveTableName]
		} else if q.Schemas["general"] != "" {
			schema = q.Schemas["general"]
		}
	}

	if schema == "" {
		return from
	}
	return schema + "." + from
}

// trim elimina espacios en blanco al inicio y final de una cadena.
// Parámetros:
//   - str: Cadena a limpiar
//
// Retorna:
//   - *string: Puntero a la cadena limpia
func trim(str string) *string {
	strEnd := strings.Trim(str, " ")
	return &strEnd
}

// trimRight elimina espacios en blanco al final de una cadena.
// Parámetros:
//   - str: Cadena a limpiar
//
// Retorna:
//   - *string: Puntero a la cadena limpia
func trimRight(str string) *string {
	strEnd := strings.TrimRight(str, " ")
	return &strEnd
}
