package sybase

import (
	"io"
	"os/exec"
	"sync"
	"time"
)

// Sybase representa una conexión a una base de datos Sybase mediante un puente Java.
// Gestiona un pool de conexiones, transacciones y la comunicación con el proceso Java.
type Sybase struct {
	// Configuración básica de conexión
	host     string // Dirección IP/hostname del servidor Sybase
	port     string // Puerto de conexión (ej: "5000")
	database string // Nombre de la base de datos de destino
	username string // Usuario para autenticación
	password string // Contraseña para autenticación

	// Configuración del pool de conexiones
	minConnections         int // Mínimo de conexiones activas en el pool (default: 1)
	maxConnections         int // Máximo de conexiones en el pool (default: 10)
	connectionTimeout      int // Tiempo máximo (segundos) para conectar (default: 30)
	idleTimeout            int // Tiempo máximo (segundos) de inactividad antes de cerrar conexión (default: 300)
	keepaliveTime          int // Intervalo (segundos) para verificar conexiones activas (default: 30)
	maxLifetime            int // Vida máxima (segundos) de una conexión (default: 3600)
	transactionConnections int // Conexiones reservadas para transacciones (default: 2)

	// Logging
	logs bool // Habilita logging detallado (default: false)

	tdsJarPath string // Ruta absoluta al archivo .jar del puente Java

	// Gestión del proceso Java
	cmd    *exec.Cmd      // Proceso del puente Java
	stdin  io.WriteCloser // Pipe para enviar comandos al proceso Java
	stdout io.ReadCloser  // Pipe para leer salida estándar del proceso
	stderr io.ReadCloser  // Pipe para leer errores del proceso

	// Estado interno
	connected        bool                       // Indica si la conexión está activa
	queryCount       int                        // Contador incremental de consultas
	currentQueries   map[int]chan QueryResponse // Canales activos por queryID
	transactionCount int                        // Contador de transacciones activas
	mu               sync.Mutex                 // Mutex para operaciones concurrentes
	config           Config                     // Configuración extendida
}

type Config struct {
	Host                   string
	Port                   string
	Database               string
	Username               string
	Password               string
	MinConnections         int
	MaxConnections         int
	ConnectionTimeout      int
	IdleTimeout            int
	KeepaliveTime          int
	MaxLifetime            int
	TransactionConnections int
	Logs                   bool
	TdsLink                string
	TdsProperties          string
	Timeout                time.Duration
}

type RawResponse struct {
	Results []map[string]any
}

type QueryRequest struct {
	MsgID       int    `json:"msgId"`
	TransID     int    `json:"transId,omitempty"`
	FinishTrans bool   `json:"finishTrans,omitempty"`
	SQL         string `json:"sql"`
}

type QueryResponse struct {
	MsgID  int    `json:"msgId,omitempty"`
	Result []any  `json:"result"`
	Error  string `json:"error,omitempty"`
}
