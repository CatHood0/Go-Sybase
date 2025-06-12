package gosybasebuilder

import (
	"strings"

)

// DeleteQuery representa una consulta DELETE de SQL y sus componentes.
// Contiene:
// - Conditions: Un slice de condiciones que forman partes de la consulta DELETE
// - Schemas: Un mapa que define esquemas de base de datos para diferentes tablas
type DeleteQuery struct {
	Conditions []Condition
	Schemas    map[string]string
}

// New crea y devuelve una nueva instancia de DeleteQuery inicializada.
func NewDelete() *DeleteQuery {
	return &DeleteQuery{Conditions: []Condition{}, Schemas: map[string]string{}}
}

// DefineSchemas establece los esquemas de base de datos para las tablas en la consulta.
//
// - schemas: Mapa donde las claves son nombres de tabla y los valores son nombres de esquema
func (q *DeleteQuery) DefineSchemas(schemas map[string]string) *DeleteQuery {
	q.Schemas = schemas
	return q
}

// From establece la tabla principal para la consulta DELETE.
//
// - from: Nombre de la tabla de la que se eliminarán registros
func (q *DeleteQuery) From(from string) *DeleteQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "delete", Query: getDeleteSchema(from, q)})
	return q
}

// Where añade una condición WHERE simple a la consulta.
//
// - where: Condición WHERE como cadena SQL
func (q *DeleteQuery) Where(where string) *DeleteQuery {
	last := q.Conditions[len(q.Conditions)-1]
	if strings.Contains(last.Query, "AND") || strings.Contains(last.Query, "OR") {
		q.Conditions = append(q.Conditions, Condition{
			TypeQuery: "continue_where",
			Query:     where,
		})
		return q
	}

	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "where",
		Query:     where,
	})
	return q
}

// WhereEquals añade una condición WHERE de igualdad.
//
// - from: Nombre de la columna
// - to: Valor a comparar
func (q *DeleteQuery) WhereEquals(from string, to string) *DeleteQuery {
	q = q.Where(from + " = " + to)
	return q
}

// WhereNotEquals añade una condición WHERE de desigualdad.
//
// - from: Nombre de la columna
// - to: Valor a comparar
func (q *DeleteQuery) WhereNotEquals(from string, to string) *DeleteQuery {
	q = q.Where(from + " != " + to)
	return q
}

// Like añade una condición WHERE con operador LIKE.
//
// - from: Nombre de la columna
// - to: Patrón de búsqueda
func (q *DeleteQuery) Like(from string, to string) *DeleteQuery {
	q = q.Where(from + " LIKE " + "'" + to + "'")
	return q
}

// NotLike añade una condición WHERE con operador NOT LIKE.
//
// - from: Nombre de la columna
// - to: Patrón de búsqueda
func (q *DeleteQuery) NotLike(from string, to string) *DeleteQuery {
	q = q.Where(from + " NOT LIKE " + "'" + to + "'")
	return q
}

// Or añade un operador OR lógico entre condiciones WHERE.
func (q *DeleteQuery) Or() *DeleteQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "args", Query: "OR"})
	return q
}

// And añade un operador AND lógico entre condiciones WHERE.
func (q *DeleteQuery) And() *DeleteQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "args", Query: "AND"})
	return q
}

// BuildSQL construye y devuelve la cadena SQL completa para la consulta DELETE.
//
// Retorna:
//   - string: La consulta SQL completa
//   - string vacío si no hay condiciones definidas
func (q *DeleteQuery) BuildSQL() string {
	conditions := q.Conditions
	if len(conditions) == 0 {
		return ""
	}
	query := "DELETE FROM "
	length := len(conditions)

	for i := range length {
		var end string = " "
		if i+1 >= length {
			end = ""
		}
		query += strings.TrimRight(conditions[i].BuildQueryStr(i+1 >= length, true), " ") + end
	}
	return query
}

// getDeleteSchema obtiene el esquema apropiado para una tabla basado en la configuración.
//
// - from: Nombre de la tabla (puede incluir alias)
// - q: Puntero a la estructura DeleteQuery que contiene los esquemas configurados
//
// Retorna:
//   - string: Nombre de tabla con esquema (si está configurado) o solo nombre de tabla
func getDeleteSchema(from string, q *DeleteQuery) string {
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
