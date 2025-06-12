package sybase

import (
	"bufio"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

func NewConnectionInstance(config Config) (*Sybase, error) {
	var tdsJarPath *string = &config.TdsLink

	if config.TdsLink == "" {
		var err error
		tdsJarPath, err = getTdsJarPath(&config)

		if err != nil {
			return nil, err
		}
	}

	return &Sybase{
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

func (s *Sybase) Connect() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return errors.New("already connected")
	}

	var cmd *exec.Cmd
	if s.config.TdsProperties != "" && checkFileExistence(s.config.TdsProperties) {
		// TdsProperties already have all the necessary configurations
		cmd = exec.Command("java", "-jar", s.tdsJarPath, s.config.TdsProperties)
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

func (s *Sybase) Disconnect() error {
	if !s.connected {
		return errors.New("Database isn't connected")
	}

	locked := s.mu.TryLock()

	if locked {
		defer s.mu.Unlock()
	}

	s.connected = false

	if s.stdin != nil {
		err := s.stdin.Close()
		if err != nil {
			return err
		}
	}
	if s.stdout != nil {
		err := s.stdout.Close()
		if err != nil {
			return err
		}
	}
	if s.stderr != nil {
		err := s.stderr.Close()
		if err != nil {
			return err
		}
	}
	if s.cmd != nil && s.cmd.Process != nil {
		err := s.cmd.Process.Kill()
		if err != nil {
			return err
		}
	}

	for _, ch := range s.currentQueries {
		close(ch)
	}
	s.currentQueries = make(map[int]chan QueryResponse)
	return nil
}
