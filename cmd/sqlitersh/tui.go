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
	titleStyle      = lipgloss.NewStyle().MarginLeft(2).MarginTop(1).Bold(true).Foreground(lipgloss.Color("205"))
	banquetBarStyle = lipgloss.NewStyle().Background(lipgloss.Color("62")).Foreground(lipgloss.Color("230")).Padding(0, 1).MarginLeft(2)
	promptStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Bold(true)
	infoStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).MarginLeft(2)
	borderStyle     = lipgloss.NewStyle().Border(lipgloss.RoundedBorder(), true, false, false, false).BorderForeground(lipgloss.Color("240"))
)

type state int

const (
	stateREPL state = iota
	statePicker
)

type model struct {
	viewport        viewport.Model
	viewportContent string
	textInput       textinput.Model
	filepicker      filepicker.Model
	state           state
	context         string
	lastURL         string
	err             error
	ready           bool

	// History navigation (↑/↓)
	history    []string
	historyIdx int
	draftInput string

	// Browser session navigation (Alt+← / Alt+→)
	navHistory []string
	navIdx     int

	// Autocomplete & Dropdown Suggestions
	suggestions   []Suggestion
	suggestionIdx int
	showDropdown  bool
	cachedDBPath  string
	cachedTables  []string
	cachedCols    map[string][]string
	activeTable   string
}

func initialModel(startCtx string) model {
	ti := textinput.New()
	ti.Placeholder = "Enter Banquet URL, table query, or command (open, help)"
	ti.Focus()
	ti.CharLimit = 1024
	ti.Width = 100
	ti.Prompt = " Banquet: "
	ti.PromptStyle = lipgloss.NewStyle().Background(lipgloss.Color("62")).Foreground(lipgloss.Color("230")).Bold(true)
	if startCtx != "" {
		ti.SetValue(startCtx)
	}

	fp := filepicker.New()
	fp.AllowedTypes = []string{".sqlite", ".db"}
	dir, _ := os.Getwd()
	fp.CurrentDirectory = dir

	vp := viewport.New(80, 20)

	m := model{
		viewport:      vp,
		textInput:     ti,
		filepicker:    fp,
		state:         stateREPL,
		context:       startCtx,
		lastURL:       startCtx,
		ready:         true,
		history:       nil,
		historyIdx:    -1,
		navHistory:    nil,
		navIdx:        -1,
		suggestions:   nil,
		suggestionIdx: -1,
		showDropdown:  false,
		cachedCols:    make(map[string][]string),
	}

	if startCtx != "" {
		m.pushNav(startCtx)
		m.refreshSchema()
		out, err := executeCommand(startCtx)
		if err != nil {
			m.err = err
			m.viewport.SetContent(fmt.Sprintf("Error: %v\n", err))
			m.viewportContent = fmt.Sprintf("Error: %v\n", err)
		} else {
			m.err = nil
			m.viewport.SetContent(out)
			m.viewportContent = out
			m.viewport.GotoTop()
		}
	}

	return m
}

func (m *model) pushNav(urlStr string) {
	if urlStr == "" {
		return
	}
	if m.navIdx >= 0 && m.navIdx < len(m.navHistory)-1 {
		m.navHistory = m.navHistory[:m.navIdx+1]
	}
	if len(m.navHistory) == 0 || m.navHistory[len(m.navHistory)-1] != urlStr {
		m.navHistory = append(m.navHistory, urlStr)
	}
	m.navIdx = len(m.navHistory) - 1
}

func (m *model) refreshSchema() {
	dbPath := extractDBPath(m.context)
	if dbPath == "" {
		dbPath = extractDBPath(m.textInput.Value())
	}
	if dbPath != "" && dbPath != m.cachedDBPath {
		m.cachedDBPath = dbPath
		tables, _ := fetchTables(dbPath)
		m.cachedTables = tables
	}
	bq, err := banquet.ParseBanquet(m.context)
	if err == nil && !bq.IsCollection && bq.Table != "" {
		m.activeTable = bq.Table
		if _, ok := m.cachedCols[bq.Table]; !ok && m.cachedDBPath != "" {
			cols, _ := fetchColumns(m.cachedDBPath, bq.Table)
			m.cachedCols[bq.Table] = cols
		}
	} else {
		m.activeTable = ""
	}
}

func (m *model) updateSuggestions() {
	m.refreshSchema()
	var cols []string
	if m.activeTable != "" {
		cols = m.cachedCols[m.activeTable]
	}
	cwd, _ := os.Getwd()
	m.suggestions = generateSuggestions(
		m.textInput.Value(),
		m.context,
		m.history,
		m.cachedTables,
		cols,
		m.activeTable,
		cwd,
	)
}

func isBackKey(key tea.KeyMsg) bool {
	if key.Alt && key.Type == tea.KeyLeft {
		return true
	}
	if key.Type == tea.KeyCtrlOpenBracket {
		return true
	}
	s := key.String()
	return s == "alt+left" || s == "ctrl+left" || s == "ctrl+["
}

func isForwardKey(key tea.KeyMsg) bool {
	if key.Alt && key.Type == tea.KeyRight {
		return true
	}
	if key.Type == tea.KeyCtrlCloseBracket {
		return true
	}
	s := key.String()
	return s == "alt+right" || s == "ctrl+right" || s == "ctrl+]"
}

func (m model) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, m.filepicker.Init())
}

func (m *model) evaluateURL(targetURL string) (tea.Model, tea.Cmd) {
	m.lastURL = targetURL
	m.context = targetURL
	m.textInput.SetValue(targetURL)
	m.pushNav(targetURL)
	m.refreshSchema()
	out, err := executeCommand(targetURL)
	if err != nil {
		m.err = err
		m.viewport.SetContent(fmt.Sprintf("Error: %v\n", err))
		m.viewportContent = fmt.Sprintf("Error: %v\n", err)
	} else {
		m.err = nil
		m.viewport.SetContent(out)
		m.viewportContent = out
		m.viewport.GotoTop()
	}
	m.textInput.Focus()
	return *m, nil
}

func (m *model) evaluateLine(line string) (tea.Model, tea.Cmd) {
	m.showDropdown = false
	m.suggestionIdx = -1
	line = strings.TrimSpace(line)
	if line == "" {
		return *m, nil
	}

	if len(m.history) == 0 || m.history[len(m.history)-1] != line {
		m.history = append(m.history, line)
	}
	m.historyIdx = -1
	m.draftInput = ""

	parts := strings.Fields(line)
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case "exit", "quit":
		return *m, tea.Quit
	case "clear":
		m.viewport.SetContent("")
		m.viewportContent = ""
		return *m, nil
	case "help":
		ht := helpText()
		m.viewport.SetContent(ht)
		m.viewportContent = ht
		m.viewport.GotoTop()
		return *m, nil
	case "open":
		if len(parts) > 1 {
			targetURL := resolveContext(m.context, parts[1])
			return m.evaluateURL(targetURL)
		}
		m.state = statePicker
		m.err = nil
		return *m, m.filepicker.Init()
	case "cd":
		if len(parts) > 1 {
			m.context = resolveContext(m.context, parts[1])
			m.lastURL = m.context
			m.textInput.SetValue(m.context)
			m.pushNav(m.context)
			m.refreshSchema()
		} else {
			m.err = fmt.Errorf("Usage: cd <path>")
		}
		return *m, nil
	case "pwd":
		m.viewport.SetContent(m.context)
		m.viewportContent = m.context
		return *m, nil
	default:
		targetURL := resolveContext(m.context, line)
		return m.evaluateURL(targetURL)
	}
}

func (m *model) handleMouseClick(x, y int) (tea.Model, tea.Cmd) {
	if y == 1 {
		m.textInput.Focus()
		return *m, nil
	}

	// 1. Dropdown items click
	if m.showDropdown && len(m.suggestions) > 0 {
		dropdownStart := 4
		dropdownEnd := dropdownStart + len(m.suggestions)
		if y >= dropdownStart && y < dropdownEnd {
			idx := y - dropdownStart
			if idx >= 0 && idx < len(m.suggestions) {
				chosen := m.suggestions[idx].Replacement
				return m.evaluateLine(chosen)
			}
		}
	}

	// 2. Viewport click
	viewportTop := 3
	if m.showDropdown && len(m.suggestions) > 0 {
		viewportTop = 5 + len(m.suggestions)
	}

	contentLineIdx := m.viewport.YOffset + (y - viewportTop)
	lines := strings.Split(m.viewportContent, "\n")
	if contentLineIdx < 0 || contentLineIdx >= len(lines) {
		return *m, nil
	}

	clickedLine := lines[contentLineIdx]

	// Collection Mode: click on database row
	if strings.Contains(clickedLine, "|") {
		parts := strings.Split(clickedLine, "|")
		if len(parts) >= 2 {
			col0 := strings.TrimSpace(parts[0])
			col1 := strings.TrimSpace(parts[1])
			if col0 != "" && col0 != "NAME" && !strings.Contains(col0, "---") {
				dbTarget := col1
				if dbTarget == "" {
					dbTarget = col0
				}
				targetURL := resolveContext(m.context, dbTarget)
				return m.evaluateURL(targetURL)
			}
		}
	}

	// Table List Mode: click on table header or rows
	var targetTable string
	for i := contentLineIdx; i >= 0 && i < len(lines); i-- {
		if strings.HasPrefix(lines[i], "=== Table: ") {
			rest := strings.TrimPrefix(lines[i], "=== Table: ")
			fields := strings.Fields(rest)
			if len(fields) > 0 {
				targetTable = fields[0]
			}
			break
		}
	}
	if targetTable != "" {
		targetURL := resolveContext(m.context, targetTable)
		return m.evaluateURL(targetURL)
	}

	return *m, nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var (
		tiCmd tea.Cmd
		vpCmd tea.Cmd
		fpCmd tea.Cmd
	)

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		headerHeight := 3
		footerHeight := 3
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

	case tea.MouseMsg:
		if msg.Action == tea.MouseActionPress {
			if msg.Button == tea.MouseButtonWheelUp {
				m.viewport.LineUp(3)
				return m, nil
			}
			if msg.Button == tea.MouseButtonWheelDown {
				m.viewport.LineDown(3)
				return m, nil
			}
			if msg.Button == tea.MouseButtonLeft {
				return m.handleMouseClick(msg.X, msg.Y)
			}
		}
	}

	if m.state == statePicker {
		m.filepicker, fpCmd = m.filepicker.Update(msg)

		if didSelect, path := m.filepicker.DidSelectFile(msg); didSelect {
			m.context = "file://" + path
			m.state = stateREPL

			out, err := executeCommand(m.context)
			m.lastURL = m.context
			m.pushNav(m.context)
			m.refreshSchema()
			if err != nil {
				m.err = err
				m.viewport.SetContent(fmt.Sprintf("Error: %v\n", err))
				m.viewportContent = fmt.Sprintf("Error: %v\n", err)
			} else {
				m.err = nil
				m.viewport.SetContent(out)
				m.viewportContent = out
				m.viewport.GotoTop()
			}
			m.textInput.SetValue(m.context)
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

	if key, ok := msg.(tea.KeyMsg); ok {
		// 1. Back / Forward Navigation (Alt+Left / Alt+Right or Ctrl+[ / Ctrl+])
		if isBackKey(key) {
			if m.navIdx > 0 {
				m.navIdx--
				targetURL := m.navHistory[m.navIdx]
				m.context = targetURL
				m.lastURL = targetURL
				m.textInput.SetValue(targetURL)
				m.textInput.SetCursor(len(targetURL))
				out, err := executeCommand(targetURL)
				if err != nil {
					m.err = err
					m.viewport.SetContent(fmt.Sprintf("Error: %v\n", err))
					m.viewportContent = fmt.Sprintf("Error: %v\n", err)
				} else {
					m.err = nil
					m.viewport.SetContent(out)
					m.viewportContent = out
					m.viewport.GotoTop()
				}
				m.showDropdown = false
				m.suggestionIdx = -1
				m.refreshSchema()
				return m, nil
			}
		}

		if isForwardKey(key) {
			if m.navIdx >= 0 && m.navIdx < len(m.navHistory)-1 {
				m.navIdx++
				targetURL := m.navHistory[m.navIdx]
				m.context = targetURL
				m.lastURL = targetURL
				m.textInput.SetValue(targetURL)
				m.textInput.SetCursor(len(targetURL))
				out, err := executeCommand(targetURL)
				if err != nil {
					m.err = err
					m.viewport.SetContent(fmt.Sprintf("Error: %v\n", err))
					m.viewportContent = fmt.Sprintf("Error: %v\n", err)
				} else {
					m.err = nil
					m.viewport.SetContent(out)
					m.viewportContent = out
					m.viewport.GotoTop()
				}
				m.showDropdown = false
				m.suggestionIdx = -1
				m.refreshSchema()
				return m, nil
			}
		}

		switch key.Type {
		case tea.KeyCtrlC:
			return m, tea.Quit

		case tea.KeyEsc:
			if m.showDropdown {
				m.showDropdown = false
				m.suggestionIdx = -1
				return m, nil
			}
			return m, tea.Quit

		case tea.KeyTab:
			if !m.showDropdown || len(m.suggestions) == 0 {
				m.updateSuggestions()
				if len(m.suggestions) > 0 {
					m.showDropdown = true
					m.suggestionIdx = 0
				}
			}
			if len(m.suggestions) > 0 {
				idx := m.suggestionIdx
				if idx < 0 || idx >= len(m.suggestions) {
					idx = 0
				}
				chosen := m.suggestions[idx].Replacement
				m.textInput.SetValue(chosen)
				m.textInput.SetCursor(len(chosen))
				m.updateSuggestions()
			}
			return m, nil

		case tea.KeyDown:
			if m.showDropdown && len(m.suggestions) > 0 {
				m.suggestionIdx++
				if m.suggestionIdx >= len(m.suggestions) {
					m.suggestionIdx = 0
				}
				return m, nil
			}
			if m.historyIdx != -1 {
				if m.historyIdx < len(m.history)-1 {
					m.historyIdx++
					m.textInput.SetValue(m.history[m.historyIdx])
					m.textInput.SetCursor(len(m.textInput.Value()))
				} else {
					m.historyIdx = -1
					m.textInput.SetValue(m.draftInput)
					m.textInput.SetCursor(len(m.textInput.Value()))
				}
				return m, nil
			}
			m.viewport, vpCmd = m.viewport.Update(msg)
			return m, vpCmd

		case tea.KeyUp:
			if m.showDropdown && len(m.suggestions) > 0 {
				m.suggestionIdx--
				if m.suggestionIdx < 0 {
					m.suggestionIdx = len(m.suggestions) - 1
				}
				return m, nil
			}
			if len(m.history) > 0 {
				if m.historyIdx == -1 {
					m.draftInput = m.textInput.Value()
					m.historyIdx = len(m.history) - 1
				} else if m.historyIdx > 0 {
					m.historyIdx--
				}
				if m.historyIdx >= 0 && m.historyIdx < len(m.history) {
					m.textInput.SetValue(m.history[m.historyIdx])
					m.textInput.SetCursor(len(m.textInput.Value()))
				}
				return m, nil
			}
			m.viewport, vpCmd = m.viewport.Update(msg)
			return m, vpCmd

		case tea.KeyPgUp, tea.KeyPgDown:
			m.viewport, vpCmd = m.viewport.Update(msg)
			return m, vpCmd

		case tea.KeyEnter:
			line := strings.TrimSpace(m.textInput.Value())
			if m.showDropdown && m.suggestionIdx >= 0 && m.suggestionIdx < len(m.suggestions) {
				line = m.suggestions[m.suggestionIdx].Replacement
			}
			return m.evaluateLine(line)
		}
	}

	prevVal := m.textInput.Value()
	m.textInput, tiCmd = m.textInput.Update(msg)

	// If input changed via typing, update suggestions and display dropdown
	if m.textInput.Value() != prevVal {
		m.updateSuggestions()
		if len(m.suggestions) > 0 {
			m.showDropdown = true
			m.suggestionIdx = -1
		} else {
			m.showDropdown = false
		}
		m.historyIdx = -1
	}

	return m, tea.Batch(tiCmd, vpCmd)
}

func renderDropdown(suggestions []Suggestion, selectedIdx int, width int) string {
	if len(suggestions) == 0 {
		return ""
	}
	boxWidth := width
	if boxWidth <= 0 || boxWidth > 100 {
		boxWidth = 90
	}

	headerStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("244")).Italic(true).MarginLeft(2)
	header := headerStyle.Render("Suggestions (Tab: complete, ↑/↓: select, Enter: open, Esc: dismiss):")

	var rows []string
	for i, s := range suggestions {
		isSel := (i == selectedIdx)
		icon := s.Type.Icon()
		cat := s.Type.Category()

		var row string
		if isSel {
			selStyle := lipgloss.NewStyle().
				Background(lipgloss.Color("62")).
				Foreground(lipgloss.Color("230")).
				Bold(true).
				Padding(0, 1)
			cursor := "▸ "
			left := fmt.Sprintf("%s%s %-24s", cursor, icon, s.Text)
			right := fmt.Sprintf("[%s] %s", cat, s.Description)
			row = selStyle.Render(fmt.Sprintf("%-48s %s", left, right))
		} else {
			normStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Padding(0, 1)
			dimStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
			cursor := "  "
			left := fmt.Sprintf("%s%s %-24s", cursor, icon, s.Text)
			right := dimStyle.Render(fmt.Sprintf("[%s] %s", cat, s.Description))
			row = normStyle.Render(left) + " " + right
		}
		rows = append(rows, "  "+row)
	}

	content := strings.Join(rows, "\n")
	dropBorder := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(lipgloss.Color("62")).
		MarginLeft(2).
		Width(boxWidth)

	return fmt.Sprintf("%s\n%s", header, dropBorder.Render(content))
}

func (m model) View() string {
	if !m.ready {
		return "\n  Initializing..."
	}

	header := titleStyle.Render("Sqlitersh")
	if m.state == stateREPL {
		header += "\n" + m.textInput.View()
		if m.showDropdown && len(m.suggestions) > 0 {
			header += "\n" + renderDropdown(m.suggestions, m.suggestionIdx, m.textInput.Width)
		}
	} else if m.lastURL != "" {
		header += "\n" + banquetBarStyle.Render(m.lastURL)
	}

	var mainContent string
	if m.state == statePicker {
		mainContent = borderStyle.Render("\n  Select a database file (ESC to cancel):\n\n" + m.filepicker.View())
	} else {
		mainContent = borderStyle.Render(m.viewport.View())
	}

	footer := ""
	if m.state == stateREPL {
		if m.err != nil {
			footer = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).MarginLeft(2).Render(m.err.Error())
		} else {
			footer = infoStyle.Render("Enter: evaluate | ↑/↓: history/suggest | Tab: complete | Alt+←/→: back/fwd | PgUp/PgDn: scroll | ctrl+c: quit")
		}
	} else {
		footer = infoStyle.Render("Use arrows to select a file | ESC to cancel")
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
  open        Interactively select a database file (or: open <file>)
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
	rel = strings.TrimSpace(rel)
	if rel == "" || rel == "." {
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

	// Check if base is a dataset (e.g. SQLite database)
	bq, err := banquet.ParseBanquet(base)
	if err == nil && !bq.IsCollection && bq.DataSetPath != "" {
		// If base already has a table, and rel is another table name without slash:
		if bq.Table != "" && !strings.Contains(rel, "/") {
			datasetURL := "file://" + bq.DataSetPath
			return datasetURL + "/" + rel
		}
	}

	if base == "file://." || base == "file://" {
		return "file://" + strings.TrimPrefix(rel, "./")
	}

	base = strings.TrimRight(base, "/")
	return base + "/" + rel
}
