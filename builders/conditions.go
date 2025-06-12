package gosybasebuilder 

// Condition representa una parte de una consulta SQL con todos sus componentes.
// Se utiliza para construir consultas SQL de manera programática.
type Condition struct {
	TypeQuery string
	Query     string
	Where     string
	Args      string
}

// BuildSelect construye y devuelve la parte SQL correspondiente a la condición,
// formateada correctamente según su tipo y posición en la consulta completa.
//
// Parámetros:
//   - isLast: Indica si esta es la última condición en la consulta (añade ";" al final)
//   - isLastColumn: Indica si es la última columna en una lista (controla comas en SELECT)
//
// Retorna:
//   - string: Fragmento SQL formateado correctamente
//
// Nota: Maneja más de 15 tipos diferentes de cláusulas SQL incluyendo:
//   - SELECT (columns), JOIN, WHERE, ORDER BY, GROUP BY
//   - INSERT (values), UPDATE (set), DELETE
//   - Cláusulas de paginación (LIMIT/OFFSET/TOP)
func (c *Condition) BuildQueryStr(isLast bool, isLastColumn bool) string {
	typeQuery := c.TypeQuery
	query := c.Query
	where := c.Where
	args := c.Args
	var end string
	if isLast {
		end = ";"
	} else {
		end = " "
	}

	switch typeQuery {
	case "columns":
		if isLastColumn {
			return query + end
		}
		return query + ", "
	case "join":
		return query + " ON " + where + end
	case "limit":
		return "TOP " + query + args + end
	case "offset":
		return "START AT " + query + args + end
	case "groupBy":
		return "GROUP BY " + query + args + end
	case "order":
		return "ORDER BY " + query + " " + args + end
	case "continue_order":
		return query + " " + args + end
	case "where":
		return "WHERE " + query + end
	case "continue_where":
		return query + end
	case "args", "primary_table_selection":
		return query + end
	case "from":
		return "FROM " + query + where + args + end
	case "to_value":
		return " VALUES " + query
	case "continue_insertions":
		return query
	case "from_update":
		// Para UPDATE: query=tabla, args=valores SET, where=condiciones WHERE
		return query + " SET " + args + " " + where + end
	case "delete":
		// Para DELETE: query=tabla, where=condiciones WHERE
		return query + " " + where + end
	default:
		return ""
	}
}
