package gosybase

import (
	"errors"
	"fmt"
)

// Row is the result of calling [DB.QueryRow] to select a single row.
type Row struct {
	// One of these two will be non-nil:
	err  error // deferred error for easy chaining
	rows Rows
}

type Rows struct {
	cols     []map[string]any
	curIndex int
	err      error
}

// Scan copies the columns from the matched row into the values
// pointed at by dest. See the documentation on [Rows.Scan] for details.
// If more than one row matches the query,
// Scan uses the first row and discards the rest. If no row matches
// the query, Scan returns [ErrNoRows].
func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}

	if !r.rows.Next() {
		if err := r.rows.Err(); err != nil {
			return err
		}
		return errors.New("No rows")
	}
	err := r.rows.Scan(dest...)
	if err != nil {
		return err
	}
	return nil
}

// Next prepares the next result row for reading with the [Rows.Scan] method. It
// returns true on success, or false if there is no next result row or an error
// happened while preparing it. [Rows.Err] should be consulted to distinguish between
// the two cases.
//
// Every call to [Rows.Scan], even the first one, must be preceded by a call to [Rows.Next].
func (rs *Rows) Next() bool {
	return rs.curIndex < len(rs.cols)
}

// Scan copies the columns in the current row into the values pointed
// at by dest. The number of values in dest must be the same as the
// number of columns in [Rows].
//
// Scan converts columns read from the database into the following
// common Go types and special types provided by the sql package:
//
//	*string
//	*[]byte
//	*int, *int8, *int16, *int32, *int64
//	*uint, *uint8, *uint16, *uint32, *uint64
//	*bool
//	*float32, *float64
//	*interface{}
//	*RawBytes
//	*Rows (cursor value)
//	any type implementing Scanner (see Scanner docs)
//
// In the most simple case, if the type of the value from the source
// column is an integer, bool or string type T and dest is of type *T,
// Scan simply assigns the value through the pointer.
//
// Scan also converts between string and numeric types, as long as no
// information would be lost. While Scan stringifies all numbers
// scanned from numeric database columns into *string, scans into
// numeric types are checked for overflow. For example, a float64 with
// value 300 or a string with value "300" can scan into a uint16, but
// not into a uint8, though float64(255) or "255" can scan into a
// uint8. One exception is that scans of some float64 numbers to
// strings may lose information when stringifying. In general, scan
// floating point columns into *float64.
//
// If a dest argument has type *[]byte, Scan saves in that argument a
// copy of the corresponding data. The copy is owned by the caller and
// can be modified and held indefinitely. The copy can be avoided by
// using an argument of type [*RawBytes] instead; see the documentation
// for [RawBytes] for restrictions on its use.
//
// If an argument has type *interface{}, Scan copies the value
// provided by the underlying driver without conversion. When scanning
// from a source value of type []byte to *interface{}, a copy of the
// slice is made and the caller owns the result.
//
// Source values of type [time.Time] may be scanned into values of type
// *time.Time, *interface{}, *string, or *[]byte. When converting to
// the latter two, [time.RFC3339Nano] is used.
//
// Source values of type bool may be scanned into types *bool,
// *interface{}, *string, *[]byte, or [*RawBytes].
//
// For scanning into *bool, the source may be true, false, 1, 0, or
// string inputs parseable by [strconv.ParseBool].
//
// Scan can also convert a cursor returned from a query, such as
// "select cursor(select * from my_table) from dual", into a
// [*Rows] value that can itself be scanned from. The parent
// select query will close any cursor [*Rows] if the parent [*Rows] is closed.
//
// If any of the first arguments implementing [Scanner] returns an error,
// that error will be wrapped in the returned error.
func (rs *Rows) Scan(dest ...any) error {
	rowsValue := rs.cols[rs.curIndex]
	var index uint = 0
	for key, value := range rowsValue {
		columnDest := dest[index]
		err := assignRowValue(&columnDest, value)
		if err != nil {
			return fmt.Errorf(`sql: Scan error on column index %d, name %q: %w`, rs.curIndex, key, err)
		}
		index += 1
	}
	rs.curIndex += 1
	return nil
}

func assignRowValue(dest *any, value any) error {
	return nil
}

// Err provides a way for wrapping packages to check for
// query errors without calling [Row.Scan].
// Err returns the error, if any, that was encountered while running the query.
// If this error is not nil, this error will also be returned from [Row.Scan].
func (r *Row) Err() error {
	return r.err
}

func (r *Rows) Err() error {
	return r.err
}
