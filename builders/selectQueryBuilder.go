package gosybasebuilder

import (
	"strings"
)

// SelectQuery representa una consulta SELECT de SQL con todas sus cláusulas.
// Permite construir consultas SELECT de manera programática mediante métodos encadenables.
type SelectQuery struct {
	Conditions               []Condition
	Schemas                  map[string]string
	lastColumnConditionIndex int
	shouldEscape             bool
}

// New crea una nueva instancia de SelectQuery inicializada y vacía.
func NewSelect() *SelectQuery {
	return &SelectQuery{Conditions: []Condition{}, Schemas: map[string]string{}}
}

// DefineSchemas configura los esquemas de base de datos para las tablas en la consulta.
// El parámetro 'schemas' es un mapa donde las claves son nombres de tabla y los valores son nombres de esquema.
// La clave especial "general" aplica un esquema por defecto a todas las tablas.
func (q *SelectQuery) DefineSchemas(schemas map[string]string) *SelectQuery {
	q.Schemas = schemas
	return q
}

func (q *SelectQuery) Escape() *SelectQuery {
	q.shouldEscape = true
	return q
}

// SelectColumns especifica las columnas a seleccionar en la consulta.
// Acepta múltiples columnas como argumentos variables.
func (q *SelectQuery) SelectColumns(columns ...string) *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "columns",
		Query:     strings.Join(columns, ", "),
	})
	q.lastColumnConditionIndex = len(columns) - 1
	return q
}

// From establece la tabla principal para la consulta.
// Aplica automáticamente el esquema correspondiente si fue definido.
func (q *SelectQuery) From(from string) *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "from", Query: getSelectSchema(from, q)})
	return q
}

// GroupBy añade una cláusula GROUP BY a la consulta.
// Ignora la operación si no se proporcionan columnas.
func (q *SelectQuery) GroupBy(columns ...string) *SelectQuery {
	if len(columns) == 1 && columns[0] == "" {
		return q
	}
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "groupBy",
		Query:     strings.Join(columns, ","),
	})
	return q
}

// Limit establece el límite de registros a devolver.
// Ignora la operación si el límite está vacío.
func (q *SelectQuery) Limit(limit string) *SelectQuery {
	if limit == "" {
		return q
	}
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "limit", Query: limit})
	return q
}

// Offset establece el desplazamiento inicial para los resultados.
// Ignora la operación si el offset está vacío.
func (q *SelectQuery) Offset(offset string) *SelectQuery {
	if offset == "" {
		return q
	}
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "offset", Query: offset})
	return q
}

// Count añade una función COUNT para una columna específica.
// Ignora la operación si la columna está vacía.
func (q *SelectQuery) Count(column string) *SelectQuery {
	if column == "" {
		return q
	}
	last := q.Conditions[len(q.Conditions)-1]

	if last.TypeQuery == "columns" {
		q.lastColumnConditionIndex++
	}
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "columns",
		Query:     "COUNT (" + column + ")",
	})
	return q
}

// CountDistinct añade una función COUNT(DISTINCT) para una columna específica.
func (q *SelectQuery) CountDistinct(column string) *SelectQuery {
	if column == "" {
		return q
	}
	return q.Count("DISTINCT " + column)
}

// Distinct aplica DISTINCT a todas las columnas seleccionadas.
func (q *SelectQuery) Distinct() *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "args",
		Query:     "DISTINCT",
	})
	return q
}

// DistinctExact aplica DISTINCT solo a una columna específica.
func (q *SelectQuery) DistinctExact(column string) *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "args",
		Query:     "DISTINCT (" + column + ")",
	})
	return q
}

// OrderBy añade una cláusula ORDER BY para una columna con tipo de orden específico.
func (q *SelectQuery) OrderBy(column string, orderType string) *SelectQuery {
	if column == "" {
		return q
	}

	last := q.Conditions[len(q.Conditions)-1]

	if last.TypeQuery == "order" || last.TypeQuery == "continue_order" {
		q.Conditions = append(q.Conditions, Condition{
			TypeQuery: "continue_order",
			Query:     ", " + column,
			Args:      orderType,
		})
		return q
	}

	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "order",
		Query:     column,
		Args:      orderType,
	})
	return q
}

// OrderByAsc añade ORDER BY con orden ascendente para una columna.
func (q *SelectQuery) OrderByAsc(column string) *SelectQuery {
	return q.OrderBy(column, "ASC")
}

// OrderByDesc añade ORDER BY con orden descendente para una columna.
func (q *SelectQuery) OrderByDesc(column string) *SelectQuery {
	return q.OrderBy(column, "DESC")
}

// Where añade una condición WHERE a la consulta.
func (q *SelectQuery) Where(where string) *SelectQuery {
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

// WhereEquals añade una condición de igualdad (=) al WHERE.
func (q *SelectQuery) WhereEquals(from string, to string) *SelectQuery {
	q = q.Where(from + " = " + to)
	return q
}

// WhereNotEquals añade una condición de desigualdad (!=) al WHERE.
func (q *SelectQuery) WhereNotEquals(from string, to string) *SelectQuery {
	q = q.Where(from + " != " + to)
	return q
}

// Like añade una condición LIKE al WHERE.
func (q *SelectQuery) Like(from string, to string) *SelectQuery {
	q = q.Where(from + " LIKE " + "'" + to + "'")
	return q
}

// NotLike añade una condición NOT LIKE al WHERE.
func (q *SelectQuery) NotLike(from string, to string) *SelectQuery {
	q = q.Where(from + " NOT LIKE " + "'" + to + "'")
	return q
}

// Or añade un operador OR lógico entre condiciones WHERE.
func (q *SelectQuery) Or() *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "args", Query: "OR"})
	return q
}

// And añade un operador AND lógico entre condiciones WHERE.
func (q *SelectQuery) And() *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{TypeQuery: "args", Query: "AND"})
	return q
}

// Join añade un JOIN genérico con tipo, tabla y condición de unión.
func (q *SelectQuery) Join(typeJoin string, from string, comparison string) *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "join",
		Query:     typeJoin + " " + getSelectSchema(from, q),
		Where:     comparison,
	})
	return q
}

// InnerJoin añade un INNER JOIN con tabla y condición de unión.
func (q *SelectQuery) InnerJoin(from string, comparison string) *SelectQuery {
	q.Join("INNER JOIN", from, comparison)
	return q
}

// LeftJoin añade un LEFT JOIN con tabla y condición de unión.
func (q *SelectQuery) LeftJoin(from string, comparison string) *SelectQuery {
	q.Join("LEFT JOIN", from, comparison)
	return q
}

// RightJoin añade un RIGHT JOIN con tabla y condición de unión.
func (q *SelectQuery) RightJoin(from string, comparison string) *SelectQuery {
	q.Join("RIGHT JOIN", from, comparison)
	return q
}

// BuildSQL construye y devuelve la cadena SQL completa.
func (q *SelectQuery) BuildSQL() string {
	conditions := q.Conditions
	if len(conditions) == 0 {
		return ""
	}
	query := "SELECT "
	length := len(conditions)

	for i := range length {
		end := ""

		if conditions[i].TypeQuery == "columns" && i+1 < length && conditions[i+1].TypeQuery == "columns" {
			end = ", "
		}

		if q.shouldEscape {
			query += EscapeJSON(conditions[i].BuildQueryStr(i+1 >= length, true) + end)
			continue
		}

		query += conditions[i].BuildQueryStr(i+1 >= length, true) + end
	}
	return query
}

// getSelectSchema aplica los esquemas definidos a los nombres de tabla.
func getSelectSchema(from string, q *SelectQuery) string {
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

// comma añade una coma para separar elementos en la consulta.
func (q *SelectQuery) comma() *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "args",
		Query:     ",",
	})
	return q
}

// whitespace añade un espacio en blanco en la consulta.
func (q *SelectQuery) whitespace() *SelectQuery {
	q.Conditions = append(q.Conditions, Condition{
		TypeQuery: "args",
		Query:     " ",
	})
	return q
}
