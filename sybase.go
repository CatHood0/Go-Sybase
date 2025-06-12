package gosybase

import (
	"bufio"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func newConnectionInstance(config Config) (*sybase, error) {
	var tdsJarPath *string = &config.TdsLink

	if config.TdsLink == "" {
		var err error
		tdsJarPath, err = getTdsJarPath(&config)

		if err != nil {
			return nil, err
		}
	}

	return &sybase{
		host:                   config.Host,
		port:                   config.Port,
		database:               config.Database,
		username:               config.Username,
		password:               config.Password,
		minConnections:         config.MinConnections,
		maxConnections:         config.MaxConnections,
		connectionTimeout:      config.ConnectionTimeout,
		idleTimeout:            config.IdleTimeout,
		keepaliveTime:          config.KeepaliveTime,
		maxLifetime:            config.KeepaliveTime,
		transactionConnections: config.TransactionConnections,
		logs:                   config.Logs,
		tdsJarPath:             *tdsJarPath,
		config:                 config,
		currentQueries:         make(map[int]chan QueryResponse),
	}, nil
}

func (s *sybase) connect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return errors.New("already connected")
	}

	var cmd *exec.Cmd
	if s.config.tdsProperties != "" && checkFileExistence(s.config.tdsProperties) {
		// tdsProperties already have all the necessary configurations
		cmd = exec.Command("java", "-jar", s.tdsJarPath, s.config.tdsProperties)
	} else {
		cmd = exec.Command("java", "-jar", s.tdsJarPath,
			s.host, s.port, s.database, s.username, s.password, strconv.FormatBool(s.logs), strconv.Itoa(s.minConnections), strconv.Itoa(s.maxConnections), strconv.Itoa(s.connectionTimeout), strconv.Itoa(s.idleTimeout), strconv.Itoa(s.keepaliveTime), strconv.Itoa(s.maxLifetime), strconv.Itoa(s.transactionConnections))
	}

	// listen any input text that will come from the commandline
	// Like StdInputReader class of TDSLink
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("error getting stdin pipe: %w", err)
	}

	// listen any log text that will comes from tds bridge
	// into the commandline
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("error getting stdout pipe: %w", err)
	}

	// listen any error text that will comes from tds bridge
	// into the commandline
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("error getting stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("error starting process: %w", err)
	}

	// if there's no logs allowed, we need to
	// check manually if the connection was handled
	// succesfully
	if !s.logs {
		scanner := bufio.NewScanner(stdout)
		if !scanner.Scan() {
			return errors.New("failed to read connection status")
		}

		if text := scanner.Text(); !strings.HasPrefix(text, "JAVALOG: Connection created") {
			return fmt.Errorf("connection failed: %s", text)
		}
	}

	s.cmd = cmd
	s.stdin = stdin
	s.stdout = stdout
	s.stderr = stderr
	s.connected = true

	go s.handleResponses()
	go s.handleErrors()

	return nil
}

func (s *sybase) disconnect() {
	if !s.connected {
		return
	}

	locked := s.mu.TryLock()

	if locked {
		defer s.mu.Unlock()
	}

	s.connected = false

	if s.stdin != nil {
		s.stdin.Close()
	}
	if s.stdout != nil {
		s.stdout.Close()
	}
	if s.stderr != nil {
		s.stderr.Close()
	}
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Kill()
	}

	for _, ch := range s.currentQueries {
		close(ch)
	}
	s.currentQueries = make(map[int]chan QueryResponse)
}
