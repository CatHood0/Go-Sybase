package sybase

import ()

type Transaction struct {
	Db        *Sybase
	TxID      int
	Finalized bool
}
