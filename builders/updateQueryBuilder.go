package gosybasebuilder

import (
	"strings"
)

// UpdateQuery representa una consulta UPDATE de SQL con sus componentes
type UpdateQuery struct {
	Conditions []Condition
	Schemas    map[string]string
}

// New crea una nueva instancia de UpdateQuery inicializada vacía
func NewUpdate() *UpdateQuery {
	return &UpdateQuery{Conditions: []Condition{}, Schemas: map[string]string{}}
}

// DefineSchemas configura los esquemas de base de datos para las tablas
// Ejemplo: map[string]string{"usuarios": "esquema_auth"}
// La clave "general" aplica a todas las tablas
func (q *UpdateQuery) DefineSchemas(schemas map[string]string) *UpdateQuery {
	q.Schemas = schemas
	return q
}

// From establece la tabla principal para la actualización
// Aplica automáticamente el esquema configurado si existe
func (q *UpdateQuery) From(from string) *UpdateQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "from_update", Query: getUpdateSchema(from, q)})
	return q
}

// SelectColumn especifica una columna y su nuevo valor para actualizar
// Ejemplo: SelectColumn("nombre", "'Juan'")
func (q *UpdateQuery) SelectColumn(column string, value string) *UpdateQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "columns",
		Query:     column + " = " + value,
	})
	return q
}

// Where añade una condición WHERE básica a la consulta
// Ejemplo: Where("edad > 18")
func (q *UpdateQuery) Where(where string) *UpdateQuery {
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

// WhereEquals añade una condición WHERE de igualdad
// Ejemplo: WhereEquals("id", "5")
func (q *UpdateQuery) WhereEquals(from string, to string) *UpdateQuery {
	q = q.Where(from + " = " + to)
	return q
}

// WhereNotEquals añade una condición WHERE de desigualdad
// Ejemplo: WhereNotEquals("estado", "'inactivo'")
func (q *UpdateQuery) WhereNotEquals(from string, to string) *UpdateQuery {
	q = q.Where(from + " != " + to)
	return q
}

// Like añade una condición WHERE con operador LIKE
// Ejemplo: Like("nombre", "%Juan%")
func (q *UpdateQuery) Like(from string, to string) *UpdateQuery {
	q = q.Where(from + " LIKE " + "'" + to + "'")
	return q
}

// NotLike añade una condición WHERE con operador NOT LIKE
// Ejemplo: NotLike("email", "%@dominio.com")
func (q *UpdateQuery) NotLike(from string, to string) *UpdateQuery {
	q = q.Where(from + " NOT LIKE " + "'" + to + "'")
	return q
}

// Or añade un operador OR entre condiciones WHERE
// Debe usarse entre llamadas a Where()
func (q *UpdateQuery) Or() *UpdateQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "args", Query: "OR"})
	return q
}

// And añade un operador AND entre condiciones WHERE
// Debe usarse entre llamadas a Where()
func (q *UpdateQuery) And() *UpdateQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "args", Query: "AND"})
	return q
}

// BuildSQL construye y devuelve la consulta SQL completa
// Retorna cadena vacía si no hay condiciones definidas
func (q *UpdateQuery) BuildSQL() string {
	conditions := q.Conditions
	if len(conditions) == 0 {
		return ""
	}
	query := "UPDATE "
	length := len(conditions)

	for i := range length {
		var connector string
		if conditions[i].TypeQuery == "columns" && i+1 < length && conditions[i+1].TypeQuery == "columns" {
			connector = ", "
		} else {
			connector = " "
		}
		query += strings.TrimRight(conditions[i].BuildQueryStr(i+1 >= length, true), " ") + connector
	}
	return query
}

// getUpdateSchema aplica los esquemas definidos a los nombres de tabla
// Maneja alias de tabla y el esquema "general" como valor por defecto
func getUpdateSchema(from string, q *UpdateQuery) string {
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
