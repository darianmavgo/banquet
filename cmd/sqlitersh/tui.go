package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/charmbracelet/bubbles/filepicker"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/darianmavgo/banquet"
	"github.com/darianmavgo/banquet/sqlite"
)

var (
	titleStyle  = lipgloss.NewStyle().MarginLeft(2).MarginTop(1).Bold(true).Foreground(lipgloss.Color("205"))
	banquetBarStyle = lipgloss.NewStyle().Background(lipgloss.Color("62")).Foreground(lipgloss.Color("230")).Padding(0, 1).MarginLeft(2)
	promptStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true)
	infoStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).MarginLeft(2)
	borderStyle = lipgloss.NewStyle().Border(lipgloss.RoundedBorder(), true, false, false, false).BorderForeground(lipgloss.Color("240"))
)

type state int

const (
	stateREPL state = iota
	statePicker
)

type model struct {
	viewport   viewport.Model
	textInput  textinput.Model
	filepicker filepicker.Model
	state      state
	context    string
	lastURL    string
	err        error
	ready      bool
}

func initialModel(startCtx string) model {
	ti := textinput.New()
	ti.Placeholder = "Enter a query, 'open', or 'help'"
	ti.Focus()
	ti.CharLimit = 256
	ti.Width = 80
	ti.PromptStyle = promptStyle

	fp := filepicker.New()
	fp.AllowedTypes = []string{".sqlite", ".db"}
	dir, _ := os.Getwd()
	fp.CurrentDirectory = dir

	return model{
		textInput:  ti,
		filepicker: fp,
		state:      stateREPL,
		context:    startCtx,
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, m.filepicker.Init())
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		tiCmd tea.Cmd
		vpCmd tea.Cmd
		fpCmd tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		headerHeight := 3 // title + banquet bar + newline
		footerHeight := 3 // input + border + info
		verticalMarginHeight := headerHeight + footerHeight

		if !m.ready {
			m.viewport = viewport.New(msg.Width, msg.Height-verticalMarginHeight)
			m.viewport.YPosition = headerHeight
			m.ready = true
		} else {
			m.viewport.Width = msg.Width
			m.viewport.Height = msg.Height - verticalMarginHeight
		}

		m.filepicker.Height = msg.Height - verticalMarginHeight - 2
	}

	if m.state == statePicker {
		m.filepicker, fpCmd = m.filepicker.Update(msg)

		if didSelect, path := m.filepicker.DidSelectFile(msg); didSelect {
			m.context = "file://" + path
			m.state = stateREPL

			out, err := executeCommand(m.context)
			m.lastURL = m.context
			if err != nil {
				m.err = err
				m.viewport.SetContent(fmt.Sprintf("Error: %v\n", err))
			} else {
				m.err = nil
				m.viewport.SetContent(out)
				m.viewport.GotoTop()
			}
			m.textInput.SetValue("")
			return m, fpCmd
		}

		if didSelect, _ := m.filepicker.DidSelectDisabledFile(msg); didSelect {
			m.err = fmt.Errorf("File type not allowed")
			return m, fpCmd
		}

		if key, ok := msg.(tea.KeyMsg); ok {
			if key.Type == tea.KeyEsc {
				m.state = stateREPL
				return m, nil
			}
			if key.Type == tea.KeyCtrlC {
				return m, tea.Quit
			}
		}
		return m, fpCmd
	}

	m.textInput, tiCmd = m.textInput.Update(msg)

	if key, ok := msg.(tea.KeyMsg); ok {
		switch key.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit

		case tea.KeyEsc:
			return m, tea.Quit // In REPL, Esc also quits? Standard in many TUIs, let's leave it. Wait, maybe just ctrl-c is safer. Let's keep it.

		case tea.KeyEnter:
			line := strings.TrimSpace(m.textInput.Value())
			if line == "" {
				return m, nil
			}
			m.textInput.SetValue("")

			parts := strings.Fields(line)
			cmd := strings.ToLower(parts[0])

			switch cmd {
			case "exit", "quit":
				return m, tea.Quit
			case "clear":
				m.viewport.SetContent("")
			case "help":
				m.viewport.SetContent(helpText())
				m.viewport.GotoTop()
			case "open":
				m.state = statePicker
				m.err = nil
				return m, m.filepicker.Init()
			case "cd":
				if len(parts) > 1 {
					m.context = resolveContext(m.context, parts[1])
				} else {
					m.err = fmt.Errorf("Usage: cd <path>")
				}
			case "pwd":
				m.viewport.SetContent(m.context)
			default:
				targetURL := resolveContext(m.context, line)
				m.lastURL = targetURL
				out, err := executeCommand(targetURL)
				if err != nil {
					m.err = err
					m.viewport.SetContent(fmt.Sprintf("Error: %v\n", err))
				} else {
					m.err = nil
					m.viewport.SetContent(out)
					m.viewport.GotoTop()
				}
			}
			return m, nil

		case tea.KeyUp, tea.KeyDown, tea.KeyPgUp, tea.KeyPgDown:
			m.viewport, vpCmd = m.viewport.Update(msg)
			return m, tea.Batch(tiCmd, vpCmd)
		}
	}

	return m, tea.Batch(tiCmd, vpCmd)
}

func (m model) View() string {
	if !m.ready {
		return "\n  Initializing..."
	}

	header := titleStyle.Render("Sqlitersh")
	if m.lastURL != "" {
		header += "\n" + banquetBarStyle.Render(m.lastURL)
	}

	var mainContent string
	if m.state == statePicker {
		mainContent = borderStyle.Render("\n  Select a database file (ESC to cancel):\n\n" + m.filepicker.View())
	} else {
		mainContent = borderStyle.Render(m.viewport.View())
	}

	promptText := getPrompt(m.context) + "> "
	m.textInput.Prompt = promptText

	footer := ""
	if m.state == stateREPL {
		footer = m.textInput.View()
		if m.err != nil {
			footer += "\n" + lipgloss.NewStyle().Foreground(lipgloss.Color("9")).MarginLeft(2).Render(m.err.Error())
		} else {
			footer += "\n" + infoStyle.Render(fmt.Sprintf("Context: %s | ↑/↓: scroll | ctrl+c: quit", m.context))
		}
	} else {
		footer = "\n" + infoStyle.Render("Use arrows to select a file | ESC to cancel")
		if m.err != nil {
			footer += "\n" + lipgloss.NewStyle().Foreground(lipgloss.Color("9")).MarginLeft(2).Render(m.err.Error())
		}
	}

	return fmt.Sprintf("%s\n%s\n%s", header, mainContent, footer)
}

func helpText() string {
	return `
Available commands:
  help        Show this help message
  exit, quit  Exit the shell
  clear       Clear the screen
  open        Interactively select a database file
  cd <path>   Change context (e.g. 'cd file:///path/to/db.sqlite')
  pwd         Show current context URL
  .           Execute query on current context

Any other input is evaluated as a Banquet query relative to the current context.
For example, if context is 'file:///db.sqlite', typing 'users' queries the 'users' table.
`
}

func executeCommand(urlStr string) (string, error) {
	bq, err := banquet.ParseBanquet(urlStr)
	if err != nil {
		return "", fmt.Errorf("parsing URL: %w", err)
	}

	if bq.IsCollection {
		return handleCollection(bq)
	}

	table := bq.Table
	if table == "" {
		table = sqlite.InferTable(bq)
	}

	if table == "sqlite_master" {
		return handleTableList(bq)
	}

	return handleQuery(bq)
}

func getPrompt(ctx string) string {
	parts := strings.Split(strings.TrimRight(ctx, "/"), "/")
	if len(parts) > 0 {
		last := parts[len(parts)-1]
		if last != "" && last != "." {
			return last
		}
	}
	return ctx
}

func resolveContext(base, rel string) string {
	if rel == "." {
		return base
	}
	if strings.HasPrefix(rel, "file://") {
		return rel
	}
	if strings.HasPrefix(rel, "/") {
		return "file://" + rel
	}
	if rel == ".." {
		idx := strings.LastIndex(base, "/")
		schemeIdx := strings.Index(base, "://")
		if idx > schemeIdx+2 {
			return base[:idx]
		}
		return base
	}

	if strings.ContainsAny(rel[:1], "[?+-;=") {
		return base + rel
	}

	base = strings.TrimRight(base, "/")
	return base + "/" + rel
}
