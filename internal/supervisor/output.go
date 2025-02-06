package supervisor

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/pkg/term/termios"
)

type ptyPipe struct {
	pty, tty *os.File
}

type multiOutput struct {
	maxNameLength int
	mutex         sync.Mutex
	pipes         map[*process]*ptyPipe
}

func (m *multiOutput) openPipe(proc *process) (pipe *ptyPipe) {
	var err error

	pipe = m.pipes[proc]

	pipe.pty, pipe.tty, err = termios.Pty()
	fatalOnErr(err)

	proc.cmd.Stdout = pipe.tty
	proc.cmd.Stderr = pipe.tty
	proc.cmd.Stdin = pipe.tty
	proc.cmd.SysProcAttr.Setctty = true
	proc.cmd.SysProcAttr.Setsid = true

	return
}

func (m *multiOutput) Connect(proc *process) {
	if len(proc.name) > m.maxNameLength {
		m.maxNameLength = len(proc.name)
	}

	if m.pipes == nil {
		m.pipes = make(map[*process]*ptyPipe)
	}

	m.pipes[proc] = &ptyPipe{}
}

func (m *multiOutput) PipeOutput(proc *process) {
	pipe := m.openPipe(proc)

	go func(proc *process, pipe *ptyPipe) {
		reader := bufio.NewReader(pipe.pty)
		for {
			line, err := reader.ReadBytes('\n')
			// Only write non-empty lines.
			if len(line) > 0 {
				m.WriteLine(proc, line)
			}
			if err != nil {
				if err != io.EOF {
					log.Printf("reader error: %v", err)
				}
				break
			}
		}
	}(proc, pipe)
}

func (m *multiOutput) WriteLine(proc *process, p []byte) {
	var buf bytes.Buffer

	color := fmt.Sprintf("\033[1;38;5;%vm", proc.color)

	buf.WriteString(color)
	buf.WriteString(proc.name)

	for buf.Len()-len(color) < m.maxNameLength {
		buf.WriteByte(' ')
	}

	buf.WriteString("\033[0m | ")

	// remove trailing newline if present.
	p = bytes.TrimSuffix(p, []byte("\n"))
	buf.Write(p)
	buf.WriteByte('\n')

	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, err := buf.WriteTo(os.Stdout)
	if err != nil {
		log.Printf("failed to write to stdout: %s", err)
	}
}

func (m *multiOutput) ClosePipe(proc *process) {
	if pipe := m.pipes[proc]; pipe != nil {
		_ = pipe.pty.Close()
		_ = pipe.tty.Close()
	}
}

func (m *multiOutput) WriteErr(proc *process, err error) {
	m.WriteLine(proc, []byte(
		fmt.Sprintf("\033[0;31m%v\033[0m", err),
	))
}
