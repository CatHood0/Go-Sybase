package gosybase

import ()

type Transaction struct {
	Db        *sybase
	TxID      int
	Finalized bool
}
